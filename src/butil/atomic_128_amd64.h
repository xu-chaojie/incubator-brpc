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

#ifndef BUTIL_ATOMIC_128_AMD_64_H
#define BUTIL_ATOMIC_128_AMD_64_H

static inline int
ntes_atomic128_cmp_exchange(ntes_int128_t *dst,
			   ntes_int128_t *exp,
			   const ntes_int128_t *src,
			   unsigned int weak ALLOW_UNUSED,
			   int success ALLOW_UNUSED ,
			   int failure ALLOW_UNUSED)
{
	uint8_t res;

	asm volatile (
		      "lock;"
		      "cmpxchg16b %[dst];"
		      " sete %[res]"
		      : [dst] "=m" (dst->val[0]),
			"=a" (exp->val[0]),
			"=d" (exp->val[1]),
			[res] "=r" (res)
		      : "b" (src->val[0]),
			"c" (src->val[1]),
			"a" (exp->val[0]),
			"d" (exp->val[1]),
			"m" (dst->val[0])
		      : "memory");

	return res;
}

#endif // BUTIL_ATOMIC_128_AMD_64_H
