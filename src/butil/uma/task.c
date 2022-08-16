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

