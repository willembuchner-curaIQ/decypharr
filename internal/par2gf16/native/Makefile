CC ?= cc
CXX ?= c++
AR ?= ar

ARCH ?= $(shell uname -m)
PARPAR := parpar
GF16 := $(PARPAR)/gf16
BUILD := build/$(ARCH)

CFLAGS := -O3 -fPIC -std=gnu99 -Wno-unused-function -funroll-loops -I$(PARPAR) -DPARPAR_INVERT_SUPPORT -DPARPAR_SLIM_GF16
CXXFLAGS := -O3 -fPIC -std=c++11 -Wno-unused-function -funroll-loops -I$(PARPAR) -DPARPAR_INVERT_SUPPORT -DPARPAR_SLIM_GF16
AVX10_FLAGS := $(shell $(CC) -x c /dev/null -c -o /dev/null -mgfni -mavx512vl -mavx512bw -mno-evex512 2>/dev/null && echo -mgfni -mavx512vl -mavx512bw -mno-evex512)

GENERIC := gf16_lookup gf16_cksum_generic gfmat_coeff gf_add_generic
SSE2 := gf_add_sse2 gf16_lookup_sse2 gf16_cksum_sse2 gf16_xor_sse2
SSSE3 := gf16_shuffle_ssse3
AVX := gf16_shuffle_avx
AVX2 := gf_add_avx2 gf16_shuffle_avx2 gf16_cksum_avx2 gf16_xor_avx2
AVX512 := gf_add_avx512 gf16_shuffle_avx512 gf16_cksum_avx512 gf16_xor_avx512
VBMI := gf16_shuffle_vbmi
GFNI := gf16_affine_avx2 gf16_affine_avx512 gf16_affine_gfni
AVX10 := gf_add_avx10 gf16_affine_avx10
NEON := gf_add_neon gf16_shuffle_neon gf16_cksum_neon gf16_clmul_neon
SHA3 := gf16_clmul_sha3
SVE := gf_add_sve gf16_shuffle128_sve gf16_cksum_sve
SVE2 := gf_add_sve2 gf16_shuffle128_sve2 gf16_shuffle2x128_sve2 gf16_shuffle512_sve2 gf16_clmul_sve2

GENERIC_OBJECTS := $(addprefix $(BUILD)/,$(addsuffix .o,$(GENERIC)))
CXX_OBJECTS := $(BUILD)/gf16mul.o $(BUILD)/bridge.o

ifeq ($(filter x86_64 amd64,$(ARCH)),)
ARCH_OBJECTS := $(addprefix $(BUILD)/,$(addsuffix .o,$(NEON) $(SHA3) $(SVE) $(SVE2)))
else
ARCH_OBJECTS := $(addprefix $(BUILD)/,$(addsuffix .o,$(SSE2) $(SSSE3) $(AVX) $(AVX2) $(AVX512) $(VBMI) $(GFNI) $(AVX10)))
endif

OBJECTS := $(GENERIC_OBJECTS) $(CXX_OBJECTS) $(ARCH_OBJECTS)

.PHONY: native darwin clean

libgf16.a: $(OBJECTS)
	$(AR) rcs $@ $^

native: libgf16.a
ifeq ($(shell uname -s),Darwin)
	cp libgf16.a libgf16_darwin.a
else ifeq ($(filter x86_64 amd64,$(ARCH)),)
	cp libgf16.a libgf16_linux_arm64.a
else
	cp libgf16.a libgf16_linux_amd64.a
endif

darwin:
	$(MAKE) clean
	$(MAKE) ARCH=x86_64 CC="cc -target x86_64-apple-macos11" CXX="c++ -target x86_64-apple-macos11" libgf16.a
	mv libgf16.a libgf16_x86_64.a
	$(MAKE) clean
	$(MAKE) ARCH=arm64 libgf16.a
	mv libgf16.a libgf16_arm64.a
	lipo -create libgf16_x86_64.a libgf16_arm64.a -output libgf16_darwin.a
	rm -f libgf16_x86_64.a libgf16_arm64.a

$(BUILD):
	mkdir -p $@

$(BUILD)/gf16mul.o: $(GF16)/gf16mul.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BUILD)/bridge.o: csrc/bridge.cpp bridge.h | $(BUILD)
	$(CXX) $(CXXFLAGS) -I. -c -o $@ $<

$(addprefix $(BUILD)/,$(addsuffix .o,$(GENERIC))): $(BUILD)/%.o: $(GF16)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(addprefix $(BUILD)/,$(addsuffix .o,$(SSE2))): $(BUILD)/%.o: $(GF16)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -msse2 -c -o $@ $<

$(BUILD)/gf16_shuffle_ssse3.o: $(GF16)/gf16_shuffle_ssse3.c | $(BUILD)
	$(CC) $(CFLAGS) -mssse3 -c -o $@ $<

$(BUILD)/gf16_shuffle_avx.o: $(GF16)/gf16_shuffle_avx.c | $(BUILD)
	$(CC) $(CFLAGS) -mavx -c -o $@ $<

$(addprefix $(BUILD)/,$(addsuffix .o,$(AVX2))): $(BUILD)/%.o: $(GF16)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -mavx2 -c -o $@ $<

$(addprefix $(BUILD)/,$(addsuffix .o,$(AVX512))): $(BUILD)/%.o: $(GF16)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -mavx512vl -mavx512bw -c -o $@ $<

$(BUILD)/gf16_shuffle_vbmi.o: $(GF16)/gf16_shuffle_vbmi.c | $(BUILD)
	$(CC) $(CFLAGS) -mavx512vl -mavx512vbmi -c -o $@ $<

$(BUILD)/gf16_affine_avx2.o: $(GF16)/gf16_affine_avx2.c | $(BUILD)
	$(CC) $(CFLAGS) -mgfni -mavx2 -c -o $@ $<

$(BUILD)/gf16_affine_avx512.o: $(GF16)/gf16_affine_avx512.c | $(BUILD)
	$(CC) $(CFLAGS) -mgfni -mavx512vl -mavx512bw -c -o $@ $<

$(BUILD)/gf16_affine_gfni.o: $(GF16)/gf16_affine_gfni.c | $(BUILD)
	$(CC) $(CFLAGS) -mgfni -mssse3 -c -o $@ $<

$(addprefix $(BUILD)/,$(addsuffix .o,$(AVX10))): $(BUILD)/%.o: $(GF16)/%.c | $(BUILD)
	$(CC) $(CFLAGS) $(AVX10_FLAGS) -c -o $@ $<

$(addprefix $(BUILD)/,$(addsuffix .o,$(NEON))): $(BUILD)/%.o: $(GF16)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(addprefix $(BUILD)/,$(addsuffix .o,$(SHA3))): $(BUILD)/%.o: $(GF16)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -march=armv8.2-a+sha3 -c -o $@ $<

$(addprefix $(BUILD)/,$(addsuffix .o,$(SVE))): $(BUILD)/%.o: $(GF16)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -march=armv8-a+sve -c -o $@ $<

$(addprefix $(BUILD)/,$(addsuffix .o,$(SVE2))): $(BUILD)/%.o: $(GF16)/%.c | $(BUILD)
	$(CC) $(CFLAGS) -march=armv8-a+sve2 -c -o $@ $<

clean:
	rm -rf build libgf16.a
