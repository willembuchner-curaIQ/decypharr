use std::hint::black_box;
use std::time::Instant;

use rayon::prelude::*;
use reedsolomon_rs::gf_simd::{
    mul_acc_input_batch_prepared, prepare_input_factor, PreparedFactorSrc, PreparedInputFactor,
};

const INPUTS: usize = 256;
const SLICE_SIZE: usize = 2_380_956;
const BATCH_SIZE: usize = 12;
const RANGE_SIZE: usize = 32 * 1024;
const RUNS: usize = 7;

fn coefficient(row: usize, column: usize) -> u16 {
    (((row + 1) * 7_919 + (column + 1) * 3_571) % 65_534 + 2) as u16
}

fn main() {
    let aligned_size = SLICE_SIZE.div_ceil(RANGE_SIZE) * RANGE_SIZE;
    let sources: Vec<Vec<u8>> = (0..INPUTS)
        .map(|source| {
            (0..aligned_size)
                .map(|offset| (source.wrapping_mul(97) + offset.wrapping_mul(31)) as u8)
                .collect()
        })
        .collect();

    println!(
        "threads={} inputs={} slice_bytes={} batch={} range={}",
        rayon::current_num_threads(),
        INPUTS,
        SLICE_SIZE,
        BATCH_SIZE,
        RANGE_SIZE,
    );
    let mut staging = vec![vec![0u8; aligned_size]; BATCH_SIZE];
    let mut staging_elapsed = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let started = Instant::now();
        for input_start in (0..INPUTS).step_by(BATCH_SIZE) {
            for (lane, input) in (input_start..(input_start + BATCH_SIZE).min(INPUTS)).enumerate() {
                staging[lane].copy_from_slice(&sources[input]);
            }
            black_box(&staging);
        }
        staging_elapsed.push(started.elapsed().as_nanos());
    }
    staging_elapsed.sort_unstable();
    let staging_median = staging_elapsed[RUNS / 2] as f64;
    println!(
        "staging ns={staging_elapsed:?} median_ns={staging_median:.0} MB_s={:.2}",
        (INPUTS * SLICE_SIZE) as f64 / staging_median * 1e3,
    );
    for output_count in [1, 4, 28] {
        let factors: Vec<Vec<PreparedInputFactor>> = (0..output_count)
            .map(|row| {
                (0..INPUTS)
                    .map(|column| prepare_input_factor(coefficient(row, column)))
                    .collect()
            })
            .collect();
        let chunks = aligned_size / RANGE_SIZE;
        let batches: Vec<Vec<Vec<PreparedFactorSrc<'_>>>> = (0..INPUTS)
            .step_by(BATCH_SIZE)
            .map(|input_start| {
                (0..chunks * output_count)
                    .map(|task| {
                        let chunk = task / output_count;
                        let row = task % output_count;
                        let offset = chunk * RANGE_SIZE;
                        (input_start..(input_start + BATCH_SIZE).min(INPUTS))
                            .map(|input| PreparedFactorSrc {
                                prepared: &factors[row][input],
                                src: &sources[input][offset..offset + RANGE_SIZE],
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect();
        let mut outputs = vec![0u8; chunks * output_count * RANGE_SIZE];
        let fold = |outputs: &mut Vec<u8>| {
            for (batch, tasks) in batches.iter().enumerate() {
                outputs
                    .par_chunks_mut(RANGE_SIZE)
                    .zip(tasks.par_iter())
                    .for_each(|(output, inputs)| {
                        if batch == 0 {
                            output.fill(0);
                        }
                        mul_acc_input_batch_prepared(output, inputs);
                    });
            }
        };

        fold(&mut outputs);
        let mut elapsed = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            let started = Instant::now();
            fold(&mut outputs);
            elapsed.push(started.elapsed().as_nanos());
        }
        elapsed.sort_unstable();
        let median = elapsed[RUNS / 2] as f64;
        let aggregate_bytes = (INPUTS * SLICE_SIZE * output_count) as f64;
        let input_bytes = (INPUTS * SLICE_SIZE) as f64;
        let checksum = outputs.iter().fold(0u8, |sum, value| sum ^ value);
        black_box(checksum);
        println!(
            "outputs={output_count} ns={elapsed:?} median_ns={median:.0} aggregate_MB_s={:.2} input_MB_s={:.2} checksum={checksum}",
            aggregate_bytes / median * 1e3,
            input_bytes / median * 1e3,
        );
    }
}
