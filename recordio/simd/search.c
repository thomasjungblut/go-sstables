#include "search.h"
#include <immintrin.h>
#include <cpuid.h>
#include <stdint.h>
#include <nmmintrin.h>  // SSE4.2

static const unsigned char pattern[] = {145, 141, 76};

// Scalar fallback implementation (no SIMD)
int find_magic_numbers_scalar(const unsigned char* data, size_t off, size_t len) {
    if (len < 3) return -1;
    if (off >= len) return -1;

    size_t end = len - 2;
    for (size_t i = off; i < end; i++) {
        if (data[i] == pattern[0] &&
            data[i+1] == pattern[1] &&
            data[i+2] == pattern[2]) {
            return i;
        }
    }
    return -1;
}

// Returns 1 if AVX2 is available, 0 otherwise
int cpu_supports_avx2() {
#ifdef _WIN32
    // Disable AVX2 on Windows due to GCC bug #54412 - stack alignment issues
    // with AVX instructions on 64-bit Windows (MinGW)
    return 0;
#endif
    unsigned int eax, ebx, ecx, edx;

    // First, check if CPUID leaf 7 is supported
    if (__get_cpuid_max(0, 0) < 7)
        return 0;

    // Call CPUID leaf 7, subleaf 0
    __cpuid_count(7, 0, eax, ebx, ecx, edx);

    // Bit 5 of EBX in CPUID leaf 7 indicates AVX2 support
    return (ebx & (1 << 5)) != 0;
}

// Returns 1 if SSE4.2 is available, 0 otherwise
int cpu_supports_sse42() {
    unsigned int eax, ebx, ecx, edx;
    // Check if CPUID leaf 1 is supported
    if (__get_cpuid_max(0, 0) < 1)
        return 0;
    
    // Call CPUID leaf 1
    __cpuid(1, eax, ebx, ecx, edx);
    
    // Bit 20 of ECX in CPUID leaf 1 indicates SSE4.2 support
    return (ecx & (1 << 20)) != 0;
}

// Returns 1 if AVX512 is available, 0 otherwise
int cpu_supports_avx512() {
#ifdef _WIN32
    // Disable AVX-512 on Windows due to GCC bug #54412 - stack alignment issues
    // with AVX instructions on 64-bit Windows (MinGW)
    return 0;
#endif
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_max(0, 0) < 7) return 0;
    __cpuid_count(7, 0, eax, ebx, ecx, edx);
    return (ebx & (1 << 16)) != 0; // AVX-512F
}

// Optimized SSE4 implementation for 3-byte pattern search
// Checks 14 positions per iteration by advancing 13 bytes at a time
// Pattern bytes are broadcast once outside the loop for better performance
int find_magic_numbers_sse4(const unsigned char* data, size_t off, size_t len) {
    if (len < 3) return -1;
    if (off >= len) return -1;
    
    size_t i = off;
    size_t end = len - 2;

    // Broadcast pattern bytes once outside the loop
    __m128i p0 = _mm_set1_epi8(pattern[0]);
    __m128i p1 = _mm_set1_epi8(pattern[1]);
    __m128i p2 = _mm_set1_epi8(pattern[2]);

    // Process 16 bytes per loop, checking 14 positions per iteration
    // Advance by 14 bytes (16 - 3 + 1) to check all positions without gaps
    for (; i + 16 <= end; i += 14) {
        __m128i d0 = _mm_loadu_si128((const __m128i*)(data + i));
        __m128i d1 = _mm_loadu_si128((const __m128i*)(data + i + 1));
        __m128i d2 = _mm_loadu_si128((const __m128i*)(data + i + 2));

        __m128i m0 = _mm_cmpeq_epi8(d0, p0);
        __m128i m1 = _mm_cmpeq_epi8(d1, p1);
        __m128i m2 = _mm_cmpeq_epi8(d2, p2);

        __m128i mask = _mm_and_si128(_mm_and_si128(m0, m1), m2);
        int matchmask = _mm_movemask_epi8(mask);

        if (matchmask) {
            // return the first match index
            return i + __builtin_ctz(matchmask);
        }
    }

    // Fallback naive scan for remaining bytes
    for (; i < end; i++) {
        if (data[i] == pattern[0] &&
            data[i+1] == pattern[1] &&
            data[i+2] == pattern[2]) {
            return i;
        }
    }

    return -1;
}

// Optimized AVX2 implementation for 3-byte pattern search
// Checks 30 positions per iteration by advancing 29 bytes at a time
// Pattern bytes are broadcast once outside the loop for better performance
int find_magic_numbers_avx2(const unsigned char* data, size_t off, size_t len) {
    if (len < 3) return -1;
    if (off >= len) return -1;

    size_t i = off;
    size_t end = len - 2;

    // Broadcast pattern bytes once outside the loop (compiler may optimize this anyway)
    __m256i p0 = _mm256_set1_epi8(pattern[0]);
    __m256i p1 = _mm256_set1_epi8(pattern[1]);
    __m256i p2 = _mm256_set1_epi8(pattern[2]);

    // Process 32 bytes per loop, checking 30 positions per iteration
    // Advance by 30 bytes (32 - 3 + 1) to check all positions without gaps
    for (; i + 32 <= end; i += 30) {
        __m256i d0 = _mm256_loadu_si256((const __m256i*)(data + i));
        __m256i d1 = _mm256_loadu_si256((const __m256i*)(data + i + 1));
        __m256i d2 = _mm256_loadu_si256((const __m256i*)(data + i + 2));

        __m256i m0 = _mm256_cmpeq_epi8(d0, p0);
        __m256i m1 = _mm256_cmpeq_epi8(d1, p1);
        __m256i m2 = _mm256_cmpeq_epi8(d2, p2);

        __m256i mask = _mm256_and_si256(_mm256_and_si256(m0, m1), m2);
        int matchmask = _mm256_movemask_epi8(mask);

        if (matchmask) {
            // return the first match index
            return i + __builtin_ctz(matchmask);
        }
    }

    // Fallback naive scan for remaining bytes
    for (; i < end; i++) {
        if (data[i] == pattern[0] &&
            data[i+1] == pattern[1] &&
            data[i+2] == pattern[2]) {
            return i;
        }
    }

    return -1;
}


// Optimized AVX512 implementation for 3-byte pattern search
// Checks 62 positions per iteration by advancing 61 bytes at a time
// Pattern bytes are broadcast once outside the loop for better performance
int find_magic_numbers_avx512(const unsigned char* data, size_t off, size_t len) {
    if (len < 3) return -1;
    if (off >= len) return -1;

    size_t i = off;
    size_t end = len - 2;

    // Broadcast pattern bytes once outside the loop
    __m512i p0 = _mm512_set1_epi8(pattern[0]);
    __m512i p1 = _mm512_set1_epi8(pattern[1]);
    __m512i p2 = _mm512_set1_epi8(pattern[2]);

    // Process 64 bytes per loop, checking 62 positions per iteration
    // Advance by 62 bytes (64 - 3 + 1) to check all positions without gaps
    for (; i + 64 <= end; i += 62) {
        __m512i d0 = _mm512_loadu_si512((const void*)(data + i));
        __m512i d1 = _mm512_loadu_si512((const void*)(data + i + 1));
        __m512i d2 = _mm512_loadu_si512((const void*)(data + i + 2));

        // AVX512 uses mask registers which are more efficient
        __mmask64 m0 = _mm512_cmpeq_epi8_mask(d0, p0);
        __mmask64 m1 = _mm512_cmpeq_epi8_mask(d1, p1);
        __mmask64 m2 = _mm512_cmpeq_epi8_mask(d2, p2);

        __mmask64 mask = m0 & m1 & m2;
        if (mask) {
            return i + __builtin_ctzll(mask);
        }
    }

    // Fallback naive scan for remaining bytes
    for (; i < end; i++) {
        if (data[i] == pattern[0] &&
            data[i+1] == pattern[1] &&
            data[i+2] == pattern[2]) {
            return i;
        }
    }

    return -1;
}
