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

#include "uma/rwlock.h"

void uma_rw_init(struct rwlock *rwl, const char *name)
{
	pthread_rwlock_init(&rwl->rw_lock, NULL);
}

void uma_rw_destroy(struct rwlock *rwl)
{
	pthread_rwlock_destroy(&rwl->rw_lock);
}

void uma_rw_rlock(struct rwlock *rwl)
{
	pthread_rwlock_rdlock(&rwl->rw_lock);
}

void uma_rw_runlock(struct rwlock *rwl)
{
	pthread_rwlock_unlock(&rwl->rw_lock);
}

void uma_rw_wlock(struct rwlock *rwl)
{
	pthread_rwlock_wrlock(&rwl->rw_lock);
}

void uma_rw_wunlock(struct rwlock *rwl)
{
	pthread_rwlock_unlock(&rwl->rw_lock);
}

