#include "uma/rwlock.h"

void uma_rw_init(struct rwlock *rwl, const char *name)
{
	pthread_rwlock_init(&rwl->rw_lock, NULL);
}

void uma_rw_destroy(struct rwlock *rwl)
{
	pthread_rwlock_destroy(&rwl->rw_lock);
}

void uma_rw_rlock(struct rwlock *rwl)
{
	pthread_rwlock_rdlock(&rwl->rw_lock);
}

void uma_rw_runlock(struct rwlock *rwl)
{
	pthread_rwlock_unlock(&rwl->rw_lock);
}

void uma_rw_wlock(struct rwlock *rwl)
{
	pthread_rwlock_wrlock(&rwl->rw_lock);
}

void uma_rw_wunlock(struct rwlock *rwl)
{
	pthread_rwlock_unlock(&rwl->rw_lock);
}

