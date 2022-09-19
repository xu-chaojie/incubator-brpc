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

#include "uma/systm.h"
#include "uma/mtx.h"

#include <errno.h>
#include <pthread.h>
#include <string.h>

#define INVALID_PTHREAD 0

void uma_mtx_init(struct mtx *m, const char *name, const char *type, int opts)
{
	pthread_mutexattr_t attr;

	pthread_mutexattr_init(&attr);
#ifdef MTX_DEBUG
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#endif
	pthread_mutex_init(&m->mtx_lock, &attr);
	pthread_mutexattr_destroy(&attr);
	m->mtx_owner = INVALID_PTHREAD;
}

void uma_mtx_destroy(struct mtx *m)
{
	m->mtx_owner = INVALID_PTHREAD;
	pthread_mutex_destroy(&m->mtx_lock);
}

int uma_mtx_trylock(struct mtx *m)
{
	int err;

	if ((err = pthread_mutex_trylock(&m->mtx_lock)) == 0) {
		m->mtx_owner = pthread_self();
		return 1;
	} else if (__predict_false(err != EBUSY)) {
		uma_panic("pthread_mutex_trylock failed (%s)", strerror(err));
	}

	return 0;
}

void uma_mtx_lock(struct mtx *m)
{
	int err;

	err = pthread_mutex_lock(&m->mtx_lock);
	if (__predict_true(err == 0)) {
		m->mtx_owner = pthread_self();
	} else {
		uma_panic("pthread_mutex_lock failed (%s)", strerror(err));
	}
}

void uma_mtx_unlock(struct mtx *m)
{
	int err;

	m->mtx_owner = INVALID_PTHREAD;
	if (__predict_false((err = pthread_mutex_unlock(&m->mtx_lock)) != 0)) {
		uma_panic("pthread_mutex_unlock failed (%s)", strerror(err));
	}
}

void uma_mtx_assert(struct mtx *m, int ma)
{
	if (ma & MA_OWNED) {
		KASSERT(m->mtx_owner == pthread_self(), ("mtx not owned"));
	}

	if (ma & MA_NOTOWNED) {
		KASSERT(m->mtx_owner != pthread_self(), ("mtx is owned"));
	}
}

void uma_mtx_sleep(pthread_cond_t *cond, struct mtx *m)
{
	if (__predict_false(m->mtx_owner != pthread_self())) {
		uma_panic("uma_mtx_sleep not owned mutex");
	}
	m->mtx_owner = INVALID_PTHREAD;
	pthread_cond_wait(cond, &m->mtx_lock);
	m->mtx_owner = pthread_self();
}

