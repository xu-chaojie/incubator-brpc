static inline int
ntes_atomic128_cmp_exchange(ntes_int128_t *dst,
			   ntes_int128_t *exp,
			   const ntes_int128_t *src,
			   unsigned int weak ALLOW_UNUSED,
			   int success ALLOW_UNUSED ,
			   int failure ALLOW_UNUSED)
{
	uint8_t res;

	asm volatile (
		      "lock;"
		      "cmpxchg16b %[dst];"
		      " sete %[res]"
		      : [dst] "=m" (dst->val[0]),
			"=a" (exp->val[0]),
			"=d" (exp->val[1]),
			[res] "=r" (res)
		      : "b" (src->val[0]),
			"c" (src->val[1]),
			"a" (exp->val[0]),
			"d" (exp->val[1]),
			"m" (dst->val[0])
		      : "memory");

	return res;
}


