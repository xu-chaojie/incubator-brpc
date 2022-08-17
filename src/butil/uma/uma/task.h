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

#ifndef UMA_TASK_H
#define UMA_TASK_H

#include <sys/queue.h>
#include <sys/param.h>
#include <stdint.h>

typedef void task_fn_t(void *context, int pending);

struct task {
	STAILQ_ENTRY(task) ta_link;
	uint16_t ta_pending;
	u_short	ta_priority;
	task_fn_t *ta_func;
	void	*ta_context;
};

#define TASK_INIT(task, priority, func, context) do {   \
        (task)->ta_pending = 0;                         \
        (task)->ta_priority = (priority);               \
        (task)->ta_func = (func);                       \
        (task)->ta_context = (context);                 \
} while (0)                                                  

void taskqueue_enqueue(struct task *task); 

#endif /* !UMA_TASK_H */
