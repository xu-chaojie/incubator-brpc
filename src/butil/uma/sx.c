#include "uma/systm.h"
#include "uma/sx.h"

#define INVALID_PTHREAD 0

void uma_sx_init(struct sx *sx, const char *name)
{
	sx->sx_xowner = INVALID_PTHREAD;
	pthread_rwlock_init(&sx->sx_lock, NULL);
}

void uma_sx_destroy(struct sx *sx)
{
	pthread_rwlock_destroy(&sx->sx_lock);
}

void uma_sx_slock(struct sx *sx)
{
	pthread_rwlock_rdlock(&sx->sx_lock);
}

void uma_sx_sunlock(struct sx *sx)
{
	pthread_rwlock_unlock(&sx->sx_lock);
}

void uma_sx_xlock(struct sx *sx)
{
	pthread_rwlock_wrlock(&sx->sx_lock);
	sx->sx_xowner = pthread_self();
}

void uma_sx_xunlock(struct sx *sx)
{
	sx->sx_xowner = INVALID_PTHREAD;
	pthread_rwlock_unlock(&sx->sx_lock);
}

void uma_sx_assert(struct sx *sx, int flags)
{
	if (flags & SA_XLOCKED) {
		KASSERT(sx->sx_owner == pthread_self(), "sx not xlocked");
	}
}
