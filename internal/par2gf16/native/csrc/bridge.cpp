#include "../bridge.h"
#include "parpar/gf16/gf16mul.h"

#include <cstring>
#include <new>

static const unsigned max_sources = 64;

struct goblack_gf16_context {
    Galois16Mul* multiplier;
    void* scratch;
    size_t slice_size;
    size_t buffer_size;
};

extern "C" {

goblack_gf16_context* goblack_gf16_create(size_t slice_size) {
    if (slice_size == 0 || (slice_size & 1)) return nullptr;
    goblack_gf16_context* context = new (std::nothrow) goblack_gf16_context;
    if (!context) return nullptr;
    Galois16Methods method = Galois16Mul::default_method(slice_size, 32768, 65535, true);
    context->multiplier = new (std::nothrow) Galois16Mul(method);
    if (!context->multiplier) {
        delete context;
        return nullptr;
    }
    context->scratch = context->multiplier->mutScratch_alloc();
    context->slice_size = slice_size;
    context->buffer_size = context->multiplier->alignToStride(slice_size);
    return context;
}

void goblack_gf16_destroy(goblack_gf16_context* context) {
    if (!context) return;
    context->multiplier->mutScratch_free(context->scratch);
    delete context->multiplier;
    delete context;
}

const char* goblack_gf16_method(goblack_gf16_context* context) {
    return context->multiplier->info().name;
}

size_t goblack_gf16_buffer_size(goblack_gf16_context* context) {
    return context->buffer_size;
}

size_t goblack_gf16_alignment(goblack_gf16_context* context) {
    return context->multiplier->info().alignment;
}

size_t goblack_gf16_stride(goblack_gf16_context* context) {
    return context->multiplier->info().stride;
}

unsigned goblack_gf16_batch_size(goblack_gf16_context* context) {
    unsigned size = context->multiplier->info().idealInputMultiple;
    if (size == 0) return 1;
    return size > max_sources ? max_sources : size;
}

void goblack_gf16_prepare(goblack_gf16_context* context, void* destination,
                          const void* source, size_t source_size) {
    if (source_size < context->buffer_size) {
        memset(static_cast<uint8_t*>(destination) + source_size, 0,
               context->buffer_size - source_size);
    }
    context->multiplier->prepare(destination, source, source_size);
}

void goblack_gf16_finish(goblack_gf16_context* context, void* buffer) {
    context->multiplier->finish(buffer, context->buffer_size);
}

int goblack_gf16_muladd_multi(goblack_gf16_context* context, unsigned count,
                              size_t offset, void* destination,
                              const void* sources, size_t source_stride,
                              size_t length, const uint16_t* coefficients) {
    if (count == 0 || count > max_sources) return 0;
    const void* source_list[max_sources];
    const uint8_t* source = static_cast<const uint8_t*>(sources);
    for (unsigned index = 0; index < count; index++) {
        source_list[index] = source + source_stride * index;
    }
    context->multiplier->mul_add_multi(count, offset, destination, source_list,
                                       length, coefficients, context->scratch);
    return 1;
}

}
