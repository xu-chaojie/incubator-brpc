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

// Authors: Xu Yifeng @ netease

#ifndef BRPC_UCP_WORKER_POOL_H
#define BRPC_UCP_WORKER_POOL_H

#include "ucp_worker.h"

#include "butil/macros.h"
#include "butil/atomicops.h"
#include "bthread/mutex.h"
#include "bthread/condition_variable.h"

#include <vector>

namespace brpc {

class UcpWorker;

class UcpWorkerPool {
public:
    UcpWorkerPool(int nworkers);
    ~UcpWorkerPool();

    int  Start();
    void Stop();
    void Barrier();

    UcpWorker *GetWorker() const;

private:
    DISALLOW_COPY_AND_ASSIGN(UcpWorkerPool);

    std::vector<UcpWorker*> workers_;
    bool started_;
    bthread::Mutex barrier_lock_;
    bthread::ConditionVariable barrier_cond_;
    int barrier_count_;
    mutable butil::atomic<unsigned> seq_; 

    class C_barrier : public EventCallback {
        UcpWorkerPool *pool;
    public:
        C_barrier(UcpWorkerPool *p): pool(p) {}
        void do_request(int id) {
            BAIDU_SCOPED_LOCK(pool->barrier_lock_);
            --pool->barrier_count_;
            pool->barrier_cond_.notify_all();
        }
    };
    friend class C_barrier;
};

} // namespace brpc
#endif
