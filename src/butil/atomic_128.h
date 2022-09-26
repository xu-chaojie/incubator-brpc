/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Author: Xu Yifeng
 */

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
