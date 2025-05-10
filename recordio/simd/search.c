#include "search.h"
#include <immintrin.h>
#include <cpuid.h>
#include <stdint.h>

static const unsigned char pattern[] = {145, 141, 76};

// Returns 1 if AVX2 is available, 0 otherwise
int cpu_supports_avx2() {
    unsigned int eax, ebx, ecx, edx;

    // First, check if CPUID leaf 7 is supported
    if (__get_cpuid_max(0, 0) < 7)
        return 0;

    // Call CPUID leaf 7, subleaf 0
    __cpuid_count(7, 0, eax, ebx, ecx, edx);

    // Bit 5 of EBX in CPUID leaf 7 indicates AVX2 support
    return (ebx & (1 << 5)) != 0;
}

// Returns 1 if AVX512 is available, 0 otherwise
int cpu_supports_avx512() {
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_max(0, 0) < 7) return 0;
    __cpuid_count(7, 0, eax, ebx, ecx, edx);
    return (ebx & (1 << 16)) != 0; // AVX-512F
}

int find_magic_numbers(const unsigned char* data, size_t off, size_t len) {
    if (len < 3) return -1;
    if (off >= len) return -1;

    size_t i = off;
    size_t end = len - 2;

    // process 32 bytes per loop using AVX2
    for (; i + 32 <= end; i += 1) {
        __m256i d0 = _mm256_loadu_si256((const __m256i*)(data + i));
        __m256i d1 = _mm256_loadu_si256((const __m256i*)(data + i + 1));
        __m256i d2 = _mm256_loadu_si256((const __m256i*)(data + i + 2));

        __m256i p0 = _mm256_set1_epi8(pattern[0]);
        __m256i p1 = _mm256_set1_epi8(pattern[1]);
        __m256i p2 = _mm256_set1_epi8(pattern[2]);

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

/*
TODO(thomas): we would need to split the cgo flags and compilation units to match

int find_magic_numbers_avx512(const unsigned char* data, size_t off, size_t len) {
    if (len < 3) return -1;
    if (off >= len) return -1;

    size_t i = off;
    size_t end = len - 2;

    // process 64 bytes per loop using AVX512
    for (size_t i = 0; i + 64 <= end; i++) {
        __m512i d0 = _mm512_loadu_si512((const void*)(data + i));
        __m512i d1 = _mm512_loadu_si512((const void*)(data + i + 1));
        __m512i d2 = _mm512_loadu_si512((const void*)(data + i + 2));

        __m512i p0 = _mm512_set1_epi8(pattern[0]);
        __m512i p1 = _mm512_set1_epi8(pattern[1]);
        __m512i p2 = _mm512_set1_epi8(pattern[2]);

        __mmask64 m0 = _mm512_cmpeq_epi8_mask(d0, p0);
        __mmask64 m1 = _mm512_cmpeq_epi8_mask(d1, p1);
        __mmask64 m2 = _mm512_cmpeq_epi8_mask(d2, p2);

        __mmask64 m = m0 & m1 & m2;
        if (m) {
            return i + __builtin_ctzll(m);
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
*/
