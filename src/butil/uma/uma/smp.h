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
#ifndef UMA_SMP_H
#define UMA_SMP_H

#include <sched.h>

#define MAXCPU 128

#define mp_maxid uma_mp_maxid()
extern int uma_mp_maxid();

extern __thread int uma_mp_bindcpu;

#define CPU_FOREACH(i)				\
        for ((i) = 0; (i) <= mp_maxid; (i)++)

#define curcpu (uma_mp_bindcpu != -1 ? uma_mp_bindcpu : sched_getcpu())
void uma_sched_bind(int cpu);
void uma_sched_unbind(void);

#define sched_bind uma_sched_bind
#define sched_unbind uma_sched_unbind

#endif
