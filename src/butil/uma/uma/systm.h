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

#ifndef UMA_SYSTM_H
#define UMA_SYSTM_H

#define INVALID_PTHREAD 0

#ifdef INVARIANTS
#define KASSERT(cond, msg)	\
	do {			\
		if (!(cond)){	\
			uma_panic msg; \
		}		\
	} while(0)
#define MPASS(cond) KASSERT(cond, (""))
#else
#define KASSERT(cond, msg)
#define MPASS(cond)
#endif

void uma_panic(const char *fmt, ...);
#define panic uma_panic

#endif // !UMA_SYSTM_H
