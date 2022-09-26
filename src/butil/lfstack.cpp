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

#include "lfstack.h"
#include "atomic_128.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

namespace butil {

LFStack::LFStack()
{
    node_buffer_ = NULL;
    head_ = free_ = {0, NULL};
    size_ = 0;
}

LFStack::LFStack(size_t max_size)
   : LFStack()
{
    int err = init(max_size);
    if (err) {
        fprintf(stderr, "LFStack::init(%ld) failed, %s\n", max_size,
                strerror(err));
        abort();
    }
}

LFStack::~LFStack()
{
    ::free(node_buffer_);
}

int LFStack::init(size_t max_size)
{
    node_buffer_ = (stack_node *)::malloc(max_size * sizeof(stack_node));
    if (node_buffer_ == NULL)
        return ENOMEM;
    for (size_t i = 0; i < max_size - 1; i++)
        node_buffer_[i].next = node_buffer_ + i + 1;
    node_buffer_[max_size - 1].next = NULL;
    free_ = { 0, node_buffer_ };
    return 0;
}

#define _i128(p) static_cast<ntes_int128_t *>((void *)(p))

inline LFStack::stack_node *LFStack::lf_pop(stack_head *head)
{
    stack_head next, orig = *head;
    do {
        if (orig.node == NULL)
            return NULL;
        next.aba = orig.aba + 1;
        next.node = orig.node->next;
    } while (!ntes_atomic128_cmp_exchange(
        _i128(head), _i128(&orig), _i128(&next), false, __ATOMIC_ACQUIRE,
        __ATOMIC_RELAXED));
    return orig.node;
}

inline void LFStack::lf_push(stack_head *head, struct stack_node *node)
{
    stack_head next, orig = *head;
    do {
        node->next = orig.node;
        next.aba = orig.aba + 1;
        next.node = node;
    } while (!ntes_atomic128_cmp_exchange(
        _i128(head), _i128(&orig), _i128(&next), false, __ATOMIC_RELEASE,
        __ATOMIC_RELAXED));
}

int LFStack::push(void *value)
{
    stack_node *node = lf_pop(&free_);
    if (node == NULL)
        return ENOMEM;
    node->value = value;
    lf_push(&head_, node);
    __atomic_fetch_add(&size_, 1, __ATOMIC_RELAXED);
    return 0;
}

void *LFStack::pop()
{
    stack_node *node = lf_pop(&head_);
    if (node == NULL)
        return NULL;
    __atomic_fetch_sub(&size_, 1, __ATOMIC_RELAXED);
    void *value = node->value;
    lf_push(&free_, node);
    return value;
}

} // namespace butil
