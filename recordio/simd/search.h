#ifndef SEARCH_H
#define SEARCH_H

#include <stddef.h>

int cpu_supports_sse42();

int cpu_supports_avx2();

int cpu_supports_avx512();

int find_magic_numbers_sse4(const unsigned char* data, size_t off, size_t len);

int find_magic_numbers_avx2(const unsigned char* data, size_t off, size_t len);

int find_magic_numbers_avx512(const unsigned char* data, size_t off, size_t len);

int find_magic_numbers_scalar(const unsigned char* data, size_t off, size_t len);

#endif