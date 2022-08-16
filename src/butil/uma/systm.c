#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

#include "uma/systm.h"

void uma_panic(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap); 
	abort();
}

