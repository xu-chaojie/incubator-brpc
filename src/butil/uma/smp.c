#include <unistd.h>
#include <stdio.h>
#include <pthread.h>

static int mp_maxid = 1024;
static pthread_once_t once = PTHREAD_ONCE_INIT;
__thread int uma_mp_bindcpu = -1;

static void get_max_cpu(void)
{
	mp_maxid = sysconf(_SC_NPROCESSORS_CONF) - 1;
}

int uma_mp_maxid()
{
	pthread_once(&once, get_max_cpu);
	return mp_maxid;
}

void uma_sched_bind(int cpu)
{
	uma_mp_bindcpu = cpu;
}

void uma_sched_unbind(void)
{
	uma_mp_bindcpu = -1;
}

