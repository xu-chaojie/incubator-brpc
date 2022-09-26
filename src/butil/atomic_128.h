#ifndef BUTIL_ATOMIC_128_H
#define BUTIL_ATOMIC_128_H

#include "macros.h"
#include <stdint.h>

namespace butil {

typedef struct {
    union {
        uint64_t val[2];
        __int128 int128;
    };
} ntes_int128_t __attribute__((__aligned__(16)));

// atomic compare and set for 128bit integer

#if defined(__aarch64__)
#include "atomic_128_aarch64.h"
#elif defined(__x86_64__) 
#include "atomic_128_amd64.h"
#else
#error "Unsupported architecture"
#endif

} // namespace butil

#endif
