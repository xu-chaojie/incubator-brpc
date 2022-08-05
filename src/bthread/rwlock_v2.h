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

#ifndef BTHREAD_RW_LOCKV2_H
#define BTHREAD_RW_LOCKV2_H

#include "bthread/types.h"
#include "butil/errno.h"
#include "butil/logging.h"

enum
{
    BTHREAD_RWLOCK_PREFER_READER_NP,
    BTHREAD_RWLOCK_PREFER_WRITER_NP, // ignored by bthread_rwlock_v2_t
    BTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP,
    BTHREAD_RWLOCK_DEFAULT_NP = BTHREAD_RWLOCK_PREFER_READER_NP
}; 

namespace bthread {
namespace v2 {

typedef struct {
    int lockkind;
} bthread_rwlockattr_t;

typedef struct bthread_rwlock {
    volatile int    rw_state;
    int             rw_flags;
    bthread_mutex_t rw_mutex;
    void            *rw_cv_reader;
    void            *rw_cv_writer;
    int             rw_blocked_readers;
    int             rw_blocked_writers;
    pthread_t       rw_thread;
    void            *rw_task;
} bthread_rwlock_t;

// -------------------------------------------
// Functions for handling read-write locks.
// -------------------------------------------

/* Initialize attribute object ATTR with default values.  */
extern int bthread_rwlockattr_init(bthread_rwlockattr_t *attr);

/* Destroy attribute object ATTR.  */
extern int bthread_rwlockattr_destroy(bthread_rwlockattr_t *attr);

/* Return current setting of reader/writer preference.  */ 
extern int bthread_rwlockattr_getkind_np(
    const bthread_rwlockattr_t* __restrict attr, int *__restrict pref);

/* Set reader/write preference.  */
extern int bthread_rwlockattr_setkind_np(bthread_rwlockattr_t *attr,
                                          int pref);

// Initialize read-write lock `rwlock' using attributes `attr', or use
// the default values if later is NULL.
extern int bthread_rwlock_init(bthread_rwlock_t* __restrict rwlock,
                               const bthread_rwlockattr_t* __restrict attr);

// Destroy read-write lock `rwlock'.
extern int bthread_rwlock_destroy(bthread_rwlock_t* rwlock);

// Acquire read lock for `rwlock'.
extern int bthread_rwlock_rdlock(bthread_rwlock_t* rwlock);

// Try to acquire read lock for `rwlock'.
extern int bthread_rwlock_tryrdlock(bthread_rwlock_t* rwlock);

// Try to acquire read lock for `rwlock' or return after specfied time.
extern int bthread_rwlock_timedrdlock(bthread_rwlock_t* __restrict rwlock,
                                      const struct timespec* __restrict abstime);

// Acquire write lock for `rwlock'.
extern int bthread_rwlock_wrlock(bthread_rwlock_t* rwlock);

// Try to acquire write lock for `rwlock'.
extern int bthread_rwlock_trywrlock(bthread_rwlock_t* rwlock);

// Try to acquire write lock for `rwlock' or return after specfied time.
extern int bthread_rwlock_timedwrlock(bthread_rwlock_t* __restrict rwlock,
                                      const struct timespec* __restrict abstime);

// Unlock `rwlock'.
extern int bthread_rwlock_unlock(bthread_rwlock_t* rwlock);


// ---------------------------------------------------
// Functions for handling read-write lock attributes.
// ---------------------------------------------------

// Initialize attribute object `attr' with default values.
extern int bthread_rwlockattr_init(bthread_rwlockattr_t* attr);

// Destroy attribute object `attr'.
extern int bthread_rwlockattr_destroy(bthread_rwlockattr_t* attr);

// Return current setting of reader/writer preference.
extern int bthread_rwlockattr_getkind_np(const bthread_rwlockattr_t* attr, int* pref);

// Set reader/write preference.
extern int bthread_rwlockattr_setkind_np(bthread_rwlockattr_t* attr, int pref);


// Specialize std::lock_guard and std::unique_lock for bthread_rwlock_t

class wlock_guard {
public:
    explicit wlock_guard(bthread_rwlock_t& mutex) : _pmutex(&mutex) {
        const int rc = bthread_rwlock_wrlock(_pmutex);
        if (rc) {
            LOG(FATAL) << "Fail to lock bthread_rwlock_t=" << _pmutex << ", " << berror(rc);
            _pmutex = NULL;
        }
    }

    ~wlock_guard() {
        unlock();
    }

    void unlock() {
        if (_pmutex) {
            bthread_rwlock_unlock(_pmutex);
            _pmutex = NULL;
        }
    }
private:
    DISALLOW_COPY_AND_ASSIGN(wlock_guard);
    bthread_rwlock_t* _pmutex;
};

class rlock_guard {
public:
    explicit rlock_guard(bthread_rwlock_t& mutex) : _pmutex(&mutex) {
        const int rc = bthread_rwlock_rdlock(_pmutex);
        if (rc) {
            LOG(FATAL) << "Fail to lock bthread_rwlock_t=" << _pmutex << ", " << berror(rc);
            _pmutex = NULL;
        }
    }

    ~rlock_guard() {
        unlock();
    }

    void unlock() {
        if (_pmutex) {
            bthread_rwlock_unlock(_pmutex);
            _pmutex = NULL;
        }
    }

private:
    DISALLOW_COPY_AND_ASSIGN(rlock_guard);
    bthread_rwlock_t* _pmutex;
};

} // namespace v2
} // namespace bthread
#endif // BTHREAD_RW_LOCKV2_H
