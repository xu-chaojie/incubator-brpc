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
#include "uma/sx.h"

void uma_sx_init(struct sx *sx, const char *name)
{
	sx->sx_xowner = INVALID_PTHREAD;
	pthread_rwlock_init(&sx->sx_lock, NULL);
}

void uma_sx_destroy(struct sx *sx)
{
	pthread_rwlock_destroy(&sx->sx_lock);
}

void uma_sx_slock(struct sx *sx)
{
	pthread_rwlock_rdlock(&sx->sx_lock);
}

void uma_sx_sunlock(struct sx *sx)
{
	pthread_rwlock_unlock(&sx->sx_lock);
}

void uma_sx_xlock(struct sx *sx)
{
	pthread_rwlock_wrlock(&sx->sx_lock);
	sx->sx_xowner = pthread_self();
}

void uma_sx_xunlock(struct sx *sx)
{
	sx->sx_xowner = INVALID_PTHREAD;
	pthread_rwlock_unlock(&sx->sx_lock);
}

void uma_sx_assert(struct sx *sx, int flags)
{
	if (flags & SA_XLOCKED) {
		KASSERT(sx->sx_owner == pthread_self(), "sx not xlocked");
	}
}
