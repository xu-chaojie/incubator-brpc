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

#ifndef UMA_SX_H
#define UMA_SX_H

#include <pthread.h>

struct sx {
	pthread_rwlock_t sx_lock;
	pthread_t sx_xowner;
};

#define SA_XLOCKED 0x01

void uma_sx_init(struct sx *rwl, const char *name);
void uma_sx_destroy(struct sx *rwl);
void uma_sx_slock(struct sx *rwl);
void uma_sx_sunlock(struct sx *rwl);
void uma_sx_xlock(struct sx *rwl);
void uma_sx_xunlock(struct sx *rwl);
void uma_sx_assert(struct sx *rwl, int flags); 

#endif
