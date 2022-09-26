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

#ifndef BUTIL_LFSTACK_H
#define BUTIL_LFSTACK_H

#include <stdlib.h>
#include <stdint.h>

namespace butil {

// lock-free fixed size stack to store pointer
class LFStack {
public:
    LFStack();
    LFStack(size_t max_size);
    ~LFStack();
    
    int init(size_t max_size);
    int push(void *value);
    void *pop(void);
    size_t size() const {
        return  __atomic_load_n(&size_, __ATOMIC_RELAXED);
    }

private:
    struct stack_node {
        void *value;
        struct stack_node *next;
    };

    // Must be same size as ntes_int128_t
    struct stack_head {
        uintptr_t aba;
        struct stack_node *node;
    };

    stack_node *node_buffer_;
    // CMPXCHG16B requires memory to be 16-bytes aligned
    stack_head head_ __attribute__((__aligned__(16)));
    stack_head free_ __attribute__((__aligned__(16)));
    size_t size_;

    static void lf_push(stack_head *head, stack_node *node);
    static stack_node *lf_pop(stack_head *head);
};

} // namespace butil
#endif // BUTIL_LFSTACK_H
