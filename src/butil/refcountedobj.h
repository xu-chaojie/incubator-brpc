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

#ifndef BUTIL_REFCOUNTEDOBJ_H
#define BUTIL_REFCOUNTEDOBJ_H

#include <butil/logging.h>
#include <butil/atomicops.h>

#include <assert.h>

namespace butil {

struct RefCountedObject {
private:
    butil::atomic<int> nref_;
public:
    RefCountedObject(int n=0) : nref_(n) {}
    virtual ~RefCountedObject()
    {
        assert(nref_ == 0);
    }

    RefCountedObject *get()
    {
        nref_.fetch_add(1, std::memory_order_relaxed);
        return this;
    }

    void put()
    {
        int v = nref_.fetch_sub(1, std::memory_order_relaxed);
        if ((v - 1) == 0)
            delete this;
    }

    int get_nref() {
        return nref_.load(std::memory_order_relaxed);
    }
};

void intrusive_ptr_add_ref(RefCountedObject *p);
void intrusive_ptr_release(RefCountedObject *p);

} //namepace butil

#endif // BUTIL_REFCOUNTEDOBJ_H
