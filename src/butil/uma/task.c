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
#include "uma/task.h"
#include <pthread.h>

static STAILQ_HEAD(taskqueue, task) task_list =
	STAILQ_HEAD_INITIALIZER(task_list);
static pthread_mutex_t task_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t task_cond = PTHREAD_COND_INITIALIZER;
static pthread_t task_thread = 0;
static void *task_routine(void *);

static void *task_routine(void *arg)
{
	struct task *t;

	pthread_mutex_lock(&task_lock);
	for (;;) {
		while (STAILQ_EMPTY(&task_list)) {
			pthread_cond_wait(&task_cond, &task_lock);
		}

		while (!STAILQ_EMPTY(&task_list)) {
			t = STAILQ_FIRST(&task_list);
			STAILQ_REMOVE_HEAD(&task_list, ta_link);
			pthread_mutex_unlock(&task_lock);
			t->ta_func(t->ta_context, t->ta_pending);
			pthread_mutex_lock(&task_lock);
		}
	}
	pthread_mutex_unlock(&task_lock);
}

void taskqueue_enqueue(struct task *task)
{
	pthread_mutex_lock(&task_lock);
	if (task_thread == 0) {
		pthread_create(&task_thread, 0, task_routine, NULL);
	}
	if (task->ta_pending) {
		task->ta_pending++;
	} else {
		task->ta_pending = 1;
		STAILQ_INSERT_HEAD(&task_list, task, ta_link);
		pthread_cond_broadcast(&task_cond);
	}
	pthread_mutex_unlock(&task_lock);
}

