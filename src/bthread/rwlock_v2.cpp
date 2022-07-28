// bthread rwlock
// Copyright (c) 2022 Netease, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Author: (yfxu@netease)

#include "butil/atomicops.h"
#include "butil/macros.h"                        // BAIDU_CASSERT
#include "bthread/rwlock_v2.h"
#include "bthread/mutex.h"
#include "bthread/condition_variable.h"
#include "bthread/task_group.h"

#define RWLOCK_WRITE_OWNER     0x80000000U
#define RWLOCK_WRITE_WAITERS   0x40000000U
#define RWLOCK_READ_WAITERS    0x20000000U
#define RWLOCK_MAX_READERS     0x1fffffffU
#define RWLOCK_READER_COUNT(c) ((c) & RWLOCK_MAX_READERS)

#define PREFER_READER(rwlock) ((rwlock)->rw_flags != 0)

namespace bthread {

using namespace butil::subtle;

extern BAIDU_THREAD_LOCAL TaskGroup* tls_task_group; 

namespace v2 {
static inline int
cas_acq_int(volatile int *dst, int expect, int src)
{
    return __atomic_compare_exchange_n(dst, &expect, src, 0,
                __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
}

static inline int
cas_rel_int(volatile int *dst, int expect, int src)
{
    return __atomic_compare_exchange_n(dst, &expect, src, 0,
                __ATOMIC_RELEASE, __ATOMIC_RELAXED);
}

int
bthread_rwlockattr_init(bthread_rwlockattr_t *attr)
{
    attr->lockkind = BTHREAD_RWLOCK_DEFAULT_NP;
    return 0;
}

int
bthread_rwlockattr_destroy(bthread_rwlockattr_t *attr)
{
    return 0;
}

int
bthread_rwlockattr_getkind_np(
    const bthread_rwlockattr_t* __restrict attr, int *__restrict pref)
{
    *pref = attr->lockkind;
    return 0;
}

int
bthread_rwlockattr_setkind_np(bthread_rwlockattr_t *attr,
                                          int pref)
{
    switch(pref) {
    case BTHREAD_RWLOCK_PREFER_WRITER_NP:
    case BTHREAD_RWLOCK_PREFER_READER_NP:
    case BTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP:
         attr->lockkind = pref;
         return 0;
    }
    return EINVAL;
}

static inline void
rwlock_set_owner(bthread_rwlock_t *rwlock)
{
    TaskGroup* g = tls_task_group;
    if (NULL == g || g->is_current_pthread_task()) {
        rwlock->rw_task = NULL;
        rwlock->rw_thread = pthread_self();
    } else {
        rwlock->rw_task = g->current_task();
        rwlock->rw_thread = 0;
    }
}

static inline int 
rwlock_check_owner(bthread_rwlock_t *rwlock)
{
    TaskGroup* g = tls_task_group;
    if (NULL == g || g->is_current_pthread_task()) {
        if (rwlock->rw_thread != pthread_self())
            return -1; 
    } else {
        if (rwlock->rw_task != g->current_task())
            return -1;
    }
    return 0;
}

static inline void
rwlock_clear_owner(bthread_rwlock_t *rwlock)
{
    rwlock->rw_task = NULL;
    rwlock->rw_thread = 0;
}

static inline int
rwlock_tryrdlock(bthread_rwlock_t *rwlock)
{
    int state;
    int wrflags;

    wrflags = RWLOCK_WRITE_OWNER;
    if (!PREFER_READER(rwlock))
        wrflags |= RWLOCK_WRITE_WAITERS;
    state = rwlock->rw_state;
    while (!(state & wrflags)) {
        if (RWLOCK_READER_COUNT(state) == RWLOCK_MAX_READERS)
            return (EAGAIN);
        if (cas_acq_int(&rwlock->rw_state, state, state + 1))
            return (0);
        state = rwlock->rw_state;
    }

    return (EBUSY);
}  	

static int
rwlock_rdwait(bthread_rwlock_t *rwlock, const struct timespec *abstime)
{
    int state;
    int wrflags;
    int error = 0;

    wrflags = RWLOCK_WRITE_OWNER;
    if (!PREFER_READER(rwlock))
        wrflags |= RWLOCK_WRITE_WAITERS;

    bthread_mutex_lock(&rwlock->rw_mutex);
    while (error == 0) {
        state = rwlock->rw_state;
        if ((state & wrflags) == 0)
            break;
        if ((state & RWLOCK_READ_WAITERS) == 0) {
            /* Set reader-waiting bit */
            if (! cas_acq_int(&rwlock->rw_state, state,
                        state | RWLOCK_READ_WAITERS))
                continue;
        }
        rwlock->rw_blocked_readers++;

        // Read butex value before unlocking
        Atomic32 v = NoBarrier_Load((Atomic32 *)rwlock->rw_cv_reader);

        // Unlock mutex
        bthread_mutex_unlock(&rwlock->rw_mutex);
        
        error = butex_wait(rwlock->rw_cv_reader, v, abstime);
        if (error) {
            error = errno;
            if (error != ETIMEDOUT) // Ignore other errors
                error = 0;
        }

        //Relock mutex
        bthread_mutex_lock(&rwlock->rw_mutex);

        if (--rwlock->rw_blocked_readers == 0) {
            /* It is lastest reader thread, clear waiting bit. */
            for (;;) {
                state = rwlock->rw_state;
                if (cas_acq_int(&rwlock->rw_state,
                            state, state & ~RWLOCK_READ_WAITERS))
                    break;
            }
        }
    }
    bthread_mutex_unlock(&rwlock->rw_mutex);
    return (error);
}

static inline int
rwlock_trywrlock(bthread_rwlock_t *rwlock)
{
    int state;

    state = rwlock->rw_state;
    while (!(state & RWLOCK_WRITE_OWNER) && RWLOCK_READER_COUNT(state) == 0) {
        if (cas_acq_int(&rwlock->rw_state, state,
                    state | RWLOCK_WRITE_OWNER)) {
            rwlock_set_owner(rwlock);
            return (0);
        }
        state = rwlock->rw_state;
    }

    return (EBUSY);
}

static int
rwlock_wrwait(bthread_rwlock_t *rwlock, const struct timespec *abstime)
{
    int state;
    int error = 0;

    bthread_mutex_lock(&rwlock->rw_mutex);
    while (error == 0) {
        state = rwlock->rw_state;
        if (!(state & RWLOCK_WRITE_OWNER) && RWLOCK_READER_COUNT(state) == 0)
            break;
        if ((state & RWLOCK_WRITE_WAITERS) == 0) {
            if (!cas_acq_int(&rwlock->rw_state, state,
                        state | RWLOCK_WRITE_WAITERS))
                continue;
        }
        rwlock->rw_blocked_writers++;

        // Read butex value before unlocking
        Atomic32 v = NoBarrier_Load((Atomic32 *)rwlock->rw_cv_writer);

        // Unlock mutex
        bthread_mutex_unlock(&rwlock->rw_mutex);
 
        error = butex_wait(rwlock->rw_cv_writer, v, abstime);
        if (error) {
            error = errno;
            if (error != ETIMEDOUT) // Ignore other errors
                error = 0;
        }

        // Relock mutex
        bthread_mutex_lock(&rwlock->rw_mutex);
 
        if (--rwlock->rw_blocked_writers == 0) {
            /* It is lastest writer thread, clear waiting bit. */
            for (;;) {
                state = rwlock->rw_state;
                if (cas_acq_int(&rwlock->rw_state,
                            state, state & ~RWLOCK_WRITE_WAITERS))
                    break;
            }
        }
    }
    bthread_mutex_unlock(&rwlock->rw_mutex);
    return (error);
}

int
bthread_rwlock_timedrdlock(bthread_rwlock_t *rwlock,
    const struct timespec *abstime)
{
    int error;

    /*
     * POSIX said the validity of the abstimeout parameter need
     * not be checked if the lock can be immediately acquired.
     */
    error = rwlock_tryrdlock(rwlock);
    if (error == 0)
        return (error);

    for (;;) {
        error = rwlock_rdwait(rwlock, abstime);
        if (rwlock_tryrdlock(rwlock) == 0) {
            error = 0;
            break;
        } else if (error != 0)
            break;
    }
    return (error);
}

int
bthread_rwlock_timedwrlock(bthread_rwlock_t *rwlock,
    const struct timespec *abstime)
{
    int error;

    error = rwlock_trywrlock(rwlock);
    if (error == 0)
        return (error);

    for (;;) {
        error = rwlock_wrwait(rwlock, abstime);
        if (rwlock_trywrlock(rwlock) == 0) {
            error = 0;
            break;
        } else if (error != 0)
            break;
    }
    return (error);
}

int
bthread_rwlock_unlock(bthread_rwlock_t *rwlock)
{
    int state;
    int broadcast = 0;
    void *q = NULL;

    state = rwlock->rw_state;
    if (state & RWLOCK_WRITE_OWNER) {
        if (rwlock_check_owner(rwlock))
            return (EPERM);
        rwlock_clear_owner(rwlock);
        for (;;) {
            if (cas_rel_int(&rwlock->rw_state, state,
                        state & ~RWLOCK_WRITE_OWNER))
                break;
            state = rwlock->rw_state;
            if (!(state & RWLOCK_WRITE_OWNER))
                return (EPERM);
        }
    } else if (RWLOCK_READER_COUNT(state) != 0) {
        for (;;) {
            if (cas_rel_int(&rwlock->rw_state, state,
                        state - 1))
                break;
            state = rwlock->rw_state;
            if (RWLOCK_READER_COUNT(state) == 0)
                return (EPERM);
        }
        if (RWLOCK_READER_COUNT(state) > 1)
            return (0);
    } else {
        return (EPERM);
    }
    q = NULL;
    broadcast = 0;
    if (!PREFER_READER(rwlock)) {
        if (state & RWLOCK_WRITE_WAITERS) {
            broadcast = 0;
            q = rwlock->rw_cv_writer;
        } else if (state & RWLOCK_READ_WAITERS) {
            broadcast = 1;
            q = rwlock->rw_cv_reader;
        }
    } else {
        if (state & RWLOCK_READ_WAITERS) {
            broadcast = 1;
            q = rwlock->rw_cv_reader;
        } else if (state & RWLOCK_WRITE_WAITERS) {
            broadcast = 0;
            q = rwlock->rw_cv_writer;
        }
    }

    if (q != NULL) {
        //bthread_mutex_lock(&rwlock->rw_mutex);
        Barrier_AtomicIncrement((Atomic32 *)q, 1);
        if (broadcast)
            butex_wake_all(q);
        else
            butex_wake(q);
        //bthread_mutex_unlock(&rwlock->rw_mutex);
    }

    return (0);
}

int
bthread_rwlock_tryrdlock(bthread_rwlock_t *rwlock)
{
    return rwlock_tryrdlock(rwlock);
}

int
bthread_rwlock_trywrlock(bthread_rwlock_t *rwlock)
{
    return rwlock_trywrlock(rwlock);
}

int
bthread_rwlock_rdlock(bthread_rwlock_t *rwlock)
{
    return bthread_rwlock_timedrdlock(rwlock, NULL);
}

int
bthread_rwlock_wrlock(bthread_rwlock_t *rwlock)
{
    return bthread_rwlock_timedwrlock(rwlock, NULL);
}

int
bthread_rwlock_init(bthread_rwlock_t *rwlock, const bthread_rwlockattr_t *attr)
{
    int rc;

    memset(rwlock, 0, sizeof(bthread_rwlock_t));
    rwlock->rw_flags = attr ?
        (attr->lockkind == BTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP) : 0;
    rc = bthread_mutex_init(&rwlock->rw_mutex, NULL);
    if (rc) {
        return rc;
    }
    rwlock->rw_cv_reader = butex_create();
    if (rwlock->rw_cv_reader == NULL) {
        bthread_mutex_destroy(&rwlock->rw_mutex);
        return -1;
    }
    NoBarrier_Store((Atomic32*)rwlock->rw_cv_reader, 0);
    rwlock->rw_cv_writer = butex_create();
    if (rwlock->rw_cv_writer == NULL) {
        butex_destroy(rwlock->rw_cv_reader);
        bthread_mutex_destroy(&rwlock->rw_mutex);
        return -1;
    }
    NoBarrier_Store((Atomic32*)rwlock->rw_cv_writer, 0);
    return (0);
}

int
bthread_rwlock_destroy(bthread_rwlock_t *rwlock)
{
    bthread_mutex_destroy(&rwlock->rw_mutex);
    butex_destroy(rwlock->rw_cv_reader);
    butex_destroy(rwlock->rw_cv_writer);
    return (0);
}
} // v2
} // namespace bthread
