# PAR2 GF(2^16) fold benchmarks

These results cover the arithmetic fold used by PAR2 reconstruction, not file
I/O, packet parsing, verification, or the complete repair pipeline. They were
recorded on 2026-08-25 from the implementation at commit `6f81ae0`.

The target workload is 256 input slices of 2,380,956 bytes folded into 28
outputs. It represents 609.52 MB of source data and 17.07 GB of coefficient
work per fold. Aggregate throughput is coefficient work divided by elapsed
time; it is not physical memory bandwidth.

## Result

| Implementation | Lifecycle | Median | Aggregate throughput | Input throughput | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GoBlack native | reusable | 96.052 ms | 177.683 GB/s | 6.346 GB/s | 184 | 1 |
| `reedsolomon-rs` 0.4.3 | kernel only | 135.324 ms | 126.117 GB/s | 4.504 GB/s | — | — |
| GoBlack native | construct, fold, close | 115.869 ms | 147.292 GB/s | 5.260 GB/s | 219,183,312 | 57 |
| `gopar-turbo` 0.2.0 native | construct and fold | 124.225 ms | 137.386 GB/s | 4.907 GB/s | 150,966,866 | 26,221 |
| GoBlack pure Go | reusable | 1,331.202 ms | 12.821 GB/s | 0.458 GB/s | 0 | 0 |
| `gopar-turbo` 0.2.0 pure Go | construct and fold | 1,648.422 ms | 10.353 GB/s | 0.370 GB/s | 5,224,312 | 5,913 |

At 28 outputs, the reusable native engine is 1.29x the throughput of
`gopar-turbo`'s native fold and 1.41x the throughput of the Rust kernel-only
measurement. Including construction, it remains 1.07x the throughput of
`gopar-turbo`. The pure-Go engine is 1.24x as fast as `gopar-turbo`'s pure-Go
path.

Output-count scaling for the reusable native engine and Rust kernel is:

| Outputs | GoBlack elapsed | GoBlack aggregate | Rust kernel elapsed | Rust kernel aggregate |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 22.562 ms | 27.015 GB/s | 9.657 ms | 63.120 GB/s |
| 4 | 30.688 ms | 79.449 GB/s | 25.045 ms | 97.350 GB/s |
| 28 | 96.052 ms | 177.683 GB/s | 135.324 ms | 126.117 GB/s |

The Rust rows deliberately favor the Rust kernel: source allocation, factor
preparation, output allocation, and source staging are outside its timed fold.
Output clearing is included. A separately timed Rust source-staging pass took
15.157 ms, or 40.214 GB/s, but it is not added to the kernel times because a
complete Rust caller could organize or overlap that work differently. The Go
benchmark includes copying every source through the input callback, output
reset, and native finalization.

## Revisions and environment

- GoBlack: commit `6f81ae0`, using the native backend vendored from
  [ParPar v0.4.5 at `458ed62`](https://github.com/animetosho/ParPar/tree/458ed62ce509fe1392830de60c8c0a7f1e14e9cf).
- Go rival: [`gopar-turbo` v0.2.0 at `9b4f539`](https://github.com/javi11/gopar-turbo/tree/9b4f53936f27e4be333269803989a2fe1c4b6132).
- Rust rival: [`reedsolomon-rs` 0.4.3 at `72cf517`](https://github.com/scryer-media/rarpar/tree/72cf51712f047a91258115859f31bb453eef094d).
- Apple M1 Pro, 10 cores, 16 GiB RAM, Darwin 25.6.0.
- Go 1.26.5, Rust 1.97.1, `GOMAXPROCS=10`, and 10 Rayon threads.

The reusable Go and `gopar-turbo` native results are medians of five samples,
with 20 folds per sample. The GoBlack native samples ranged from 94.283 ms to
96.460 ms; the rival samples ranged from 119.477 ms to 126.218 ms. The cold
result is the median of seven single folds, and the pure-Go result is the
median of five single folds. The Rust result is the median of three independent
process medians; each process measured seven folds after an untimed warm-up.
Inputs and coefficients are deterministic and created before timing. No
benchmark performs disk or network I/O. Absolute rates vary with host load and
power state, so comparisons should be rerun together on the target machine.

## Reproduction

Run the GoBlack benchmarks from the repository root:

```sh
GOMAXPROCS=10 go test ./internal/par2gf16 -run '^$' -bench '^BenchmarkFolder/28-outputs$' -benchtime=20x -count=5 -benchmem
GOMAXPROCS=10 go test ./internal/par2gf16 -run '^$' -bench '^BenchmarkFolderCold$' -benchtime=1x -count=7 -benchmem
GOMAXPROCS=10 CGO_ENABLED=0 go test ./internal/par2gf16 -run '^$' -bench '^BenchmarkFolder/28-outputs$' -benchtime=1x -count=5 -benchmem
```

Run the pinned Go rival:

```sh
git clone https://github.com/javi11/gopar-turbo /tmp/gopar-turbo-v0.2.0
git -C /tmp/gopar-turbo-v0.2.0 checkout 9b4f53936f27e4be333269803989a2fe1c4b6132
cd /tmp/gopar-turbo-v0.2.0
GOMAXPROCS=10 go test ./rsec16 -run '^$' -bench '^BenchmarkFoldInputs_Realistic$' -benchtime=20x -count=5 -benchmem
GOMAXPROCS=10 CGO_ENABLED=0 go test ./rsec16 -run '^$' -bench '^BenchmarkFoldInputs_Realistic$' -benchtime=1x -count=5 -benchmem
```

Run the pinned Rust rival from the GoBlack repository root:

```sh
git clone https://github.com/scryer-media/rarpar /tmp/rarpar
git -C /tmp/rarpar checkout 72cf51712f047a91258115859f31bb453eef094d
cp internal/par2gf16/benchmarks/rarpar_fold.rs /tmp/rarpar/crates/reedsolomon-rs/examples/goblack_fold_bench.rs
cd /tmp/rarpar
RAYON_NUM_THREADS=10 cargo run --locked --release -p reedsolomon-rs --example goblack_fold_bench
```

Repeat the final command in three fresh processes and compare the median each
prints. The harness pads each source to a 32 KiB boundary because the public
Rust SIMD API requires fixed-size chunks. Throughput is charged only against
the original unpadded size, making the reported Rust rate slightly
conservative by approximately 0.47%.
