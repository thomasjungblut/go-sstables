#ifndef SEARCH_H
#define SEARCH_H

#include <stddef.h>

int cpu_supports_avx2();

int cpu_supports_avx512();

int find_magic_numbers(const unsigned char* data, size_t off, size_t len);

#endif