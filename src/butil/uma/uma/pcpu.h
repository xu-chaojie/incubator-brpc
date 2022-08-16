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
#ifndef UMA_PCPU_H
#define UMA_PCPU_H

#include <sys/user.h>
#include "smp.h"

struct pcpu {
	char pad[PAGE_SIZE];
};

/* Accessor to elements allocated via UMA_ZONE_PCPU zone. */
static inline void *
zpcpu_get(void *base)
{

        return ((char *)(base) + sizeof(struct pcpu) * curcpu);
}

static inline void *
zpcpu_get_cpu(void *base, int cpu)
{

        return ((char *)(base) + sizeof(struct pcpu) * cpu);
}
#endif
