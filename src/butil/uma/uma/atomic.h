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

#ifndef UMA_ATOMIC_H
#define UMA_ATOMIC_H

#include <sys/param.h>

static inline void atomic_clear_long(volatile u_long *w, u_long m) {
	__atomic_fetch_and(w, ~m, __ATOMIC_RELAXED);
}

static inline void atomic_set_long(volatile u_long *w, u_long m) {
	__atomic_fetch_or(w, m, __ATOMIC_RELAXED);
}

static inline void atomic_add_long(volatile u_long *w, u_long m) {
	__atomic_fetch_add(w, m, __ATOMIC_RELAXED);
}

static inline long atomic_fetchadd_long(volatile u_long *w, u_long m) {
	return __atomic_fetch_add(w, m, __ATOMIC_RELAXED);
}

static inline void atomic_set_acq_long(volatile u_long *w, u_long m) {
	__atomic_fetch_or(w, m, __ATOMIC_ACQUIRE);
}

static inline void atomic_store_rel_long(volatile u_long *w, u_long v) {
	__atomic_store_n(w, v, __ATOMIC_RELEASE);
}

#endif // !UMA_ATOMIC_H
