static inline int
atomic128_cmp_exchange(ntes_int128_t *dst, ntes_int128_t *exp,
    const ntes_int128_t *src, unsigned int weak, int success, 
    int failure  ALLOW_UNUSED)
{
    ntes_int128_t expected = *exp;
    ntes_int128_t desired = *src;
    ntes_int128_t old;
#define __HAS_ACQ(mo) ((mo) != __ATOMIC_RELAXED && (mo) != __ATOMIC_RELEASE)
#define __HAS_RLS(mo) ((mo) == __ATOMIC_RELEASE || (mo) == __ATOMIC_ACQ_REL || \
 (mo) == __ATOMIC_SEQ_CST)
    int ldx_mo = __HAS_ACQ(success) ? __ATOMIC_ACQUIRE : __ATOMIC_RELAXED;
    int stx_mo = __HAS_RLS(success) ? __ATOMIC_RELEASE : __ATOMIC_RELAXED;
#undef __HAS_ACQ
#undef __HAS_RLS

    uint32_t ret = 1;
	/* ldx128 can not guarantee atomic,
	 * Must write back src or old to verify atomicity of ldx128;
	 */
	do {

#define __LOAD_128(op_string, src, dst) { \
	asm volatile(                     \
		op_string " %0, %1, %2"   \
		: "=&r" (dst.val[0]),     \
		  "=&r" (dst.val[1])      \
		: "Q" (src->val[0])       \
		: "memory"); }

		if (ldx_mo == __ATOMIC_RELAXED)
			__LOAD_128("ldxp", dst, old)
		else
			__LOAD_128("ldaxp", dst, old)

#undef __LOAD_128

#define __STORE_128(op_string, dst, src, ret) { \
	asm volatile(                           \
		op_string " %w0, %1, %2, %3"    \
		: "=&r" (ret)                   \
		: "r" (src.val[0]),             \
		  "r" (src.val[1]),             \
		  "Q" (dst->val[0])             \
		: "memory"); }

		if (BAIDU_LIKELY(old.int128 == expected.int128)) {
			if (stx_mo == __ATOMIC_RELAXED)
				__STORE_128("stxp", dst, desired, ret)
			else
				__STORE_128("stlxp", dst, desired, ret)
		} else {
			/* In the failure case (since 'weak' is ignored and only
			 * weak == 0 is implemented), expected should contain
			 * the atomically read value of dst. This means, 'old'
			 * needs to be stored back to ensure it was read
			 * atomically.
			 */
			if (stx_mo == __ATOMIC_RELAXED)
				__STORE_128("stxp", dst, old, ret)
			else
				__STORE_128("stlxp", dst, old, ret)
		}

#undef __STORE_128

	} while (BAIDU_UNLIKELY(ret));

	/* Unconditionally updating the value of exp removes an 'if' statement.
	 * The value of exp should already be in register if not in the cache.
	 */
	*exp = old;
	return (old.int128 == expected.int128);
}

