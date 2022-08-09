/*-
 * SPDX-License-Identifier: BSD-2-Clause-FreeBSD
 *
 * Copyright (c) 2013 EMC Corp.
 * Copyright (c) 2011 Jeffrey Roberson <jeff@freebsd.org>
 * Copyright (c) 2008 Mayur Shardul <mayur.shardul@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * $FreeBSD$
 */

#ifndef _BUTIL_PCTRIE_H_
#define _BUTIL_PCTRIE_H_

#include <stdint.h>
#include <stddef.h>
#include "butil/macros.h"

#include "_pctrie.h"

namespace butil {

typedef bool boolean_t;

#define CTASSERT(expr) BAIDU_CASSERT(expr, #expr)

#define	PCTRIE_DEFINE(name, type, field, allocfn, freefn)		\
									\
CTASSERT(sizeof(((struct type *)0)->field) == sizeof(uint64_t));	\
/*									\
 * XXX This assert protects flag bits, it does not enforce natural	\
 * alignment.  32bit architectures do not naturally align 64bit fields.	\
 */									\
CTASSERT((offsetof(struct type, field) & (sizeof(uint32_t) - 1)) == 0); \
									\
static __inline struct type *						\
name##_PCTRIE_VAL2PTR(uint64_t *val)					\
{									\
									\
	if (val == NULL)						\
		return (NULL);						\
	return (struct type *)						\
	    ((uintptr_t)val - offsetof(struct type, field));		\
}									\
									\
static __inline uint64_t *						\
name##_PCTRIE_PTR2VAL(struct type *ptr)					\
{									\
									\
	return &ptr->field;						\
}									\
									\
static __inline int							\
name##_PCTRIE_INSERT(struct butil::pctrie *ptree, struct type *ptr)	\
{									\
									\
	return butil::pctrie_insert(ptree, name##_PCTRIE_PTR2VAL(ptr),	\
	    allocfn);							\
}									\
									\
static __inline struct type *						\
name##_PCTRIE_LOOKUP(struct butil::pctrie *ptree, uint64_t key)		\
{									\
									\
	return name##_PCTRIE_VAL2PTR(butil::pctrie_lookup(ptree, key));	\
}									\
									\
static __inline __attribute__((unused)) struct type *			\
name##_PCTRIE_LOOKUP_LE(struct butil::pctrie *ptree, uint64_t key)	\
{									\
									\
	return name##_PCTRIE_VAL2PTR(butil::pctrie_lookup_le(ptree, key));	\
}									\
									\
static __inline __attribute__((unused)) struct type *			\
name##_PCTRIE_LOOKUP_GE(struct butil::pctrie *ptree, uint64_t key)	\
{									\
									\
	return name##_PCTRIE_VAL2PTR(butil::pctrie_lookup_ge(ptree, key));	\
}									\
									\
static __inline __attribute__((unused))  void				\
name##_PCTRIE_RECLAIM(struct butil::pctrie *ptree)			\
{									\
									\
	butil::pctrie_reclaim_allnodes(ptree, freefn);			\
}									\
									\
static __inline void							\
name##_PCTRIE_REMOVE(struct butil::pctrie *ptree, uint64_t key)		\
{									\
									\
	butil::pctrie_remove(ptree, key, freefn);			\
}

typedef	void	*(*pctrie_alloc_t)(struct pctrie *ptree);
typedef	void 	(*pctrie_free_t)(struct pctrie *ptree, void *node);

int		pctrie_insert(struct pctrie *ptree, uint64_t *val, 
		    pctrie_alloc_t allocfn);
uint64_t	*pctrie_lookup(struct pctrie *ptree, uint64_t key);
uint64_t	*pctrie_lookup_ge(struct pctrie *ptree, uint64_t key);
uint64_t	*pctrie_lookup_le(struct pctrie *ptree, uint64_t key);
void		pctrie_reclaim_allnodes(struct pctrie *ptree,
		    pctrie_free_t freefn);
void		pctrie_remove(struct pctrie *ptree, uint64_t key,
		    pctrie_free_t freefn);
size_t		pctrie_node_size(void);
int		pctrie_zone_init(void *mem, int size, int flags);

static __inline void
pctrie_init(struct pctrie *ptree)
{

	ptree->pt_root = 0;
}

static __inline boolean_t
pctrie_is_empty(struct pctrie *ptree)
{

	return (ptree->pt_root == 0);
}

/*
 * These widths should allow the pointers to a node's children to fit within
 * a single cache line.  The extra levels from a narrow width should not be
 * a problem thanks to path compression.
 */
#ifdef __LP64__
#define	PCTRIE_WIDTH	4
#else
#define	PCTRIE_WIDTH	3
#endif

#define	PCTRIE_COUNT	(1 << PCTRIE_WIDTH)

} // namespace butil

#endif /* !_BUTIL_PCTRIE_H_ */
