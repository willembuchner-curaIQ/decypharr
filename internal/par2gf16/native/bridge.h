#ifndef GOBLACK_PAR2_GF16_BRIDGE_H
#define GOBLACK_PAR2_GF16_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct goblack_gf16_context goblack_gf16_context;

goblack_gf16_context* goblack_gf16_create(size_t slice_size);
void goblack_gf16_destroy(goblack_gf16_context* context);

const char* goblack_gf16_method(goblack_gf16_context* context);
size_t goblack_gf16_buffer_size(goblack_gf16_context* context);
size_t goblack_gf16_alignment(goblack_gf16_context* context);
size_t goblack_gf16_stride(goblack_gf16_context* context);
unsigned goblack_gf16_batch_size(goblack_gf16_context* context);

void goblack_gf16_prepare(goblack_gf16_context* context, void* destination,
                          const void* source, size_t source_size);
void goblack_gf16_finish(goblack_gf16_context* context, void* buffer);
int goblack_gf16_muladd_multi(goblack_gf16_context* context, unsigned count,
                              size_t offset, void* destination,
                              const void* sources, size_t source_stride,
                              size_t length, const uint16_t* coefficients);

#ifdef __cplusplus
}
#endif

#endif
