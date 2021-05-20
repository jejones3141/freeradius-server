/*
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program; if not, write to the Free Software
 *   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 */

/** Functions for a quickheap
 *
 * @file src/lib/util/quickheap.c
 *
 * @copyright 2021 The FreeRADIUS server project
 */
RCSID()

#include <freeradius-devel/util/quickheap.h>

#include <freeradius-devel/util/rand.h>

/*
 * "Quickheaps: Simple, Efficient, and Cache-Oblivious" by Gonzalo Navarro and
 * Rodrigo Paredes defines the quickheap structure. It's nearly as simple as
 * the binary heap, but more cache-friendly.
 */

static void incremental_quicksort(fr_quickheap_t *qh);
static size_t partition(fr_quickheap_t *qh, void *pivot, size_t low, size_t high);
 
/*
 * The quickheap as defined in the paper have a fixed size set at creation.
 * We will ultimately let them grow as the current fr_heap_t can.
 */
#define INITIAL_CAPACITY	2048

/*
 * The quickheap heap is treated as a circular array, hence this macro.
 * It should be usable where an lvalue is required.
 */
#define heap_sub(qh, n)	((qh)->heap[(n) % (qh)->capacity])

struct fr_quickheap_t {
	size_t		capacity;
	size_t		idx;
	void		**heap;
	stack_t		s;		/* a stack of indices of pivots */
	fr_fast_rand_t	rand_ctx;	/* for random choice of pivot in incremental_quicksort */
	char const	*type; 
	fr_heap_cmp_t	cmp;
};

fr_quickheap_t *_fr_quickheap_alloc(TALLOC_CTX *ctx, fr_heap_cmp_t cmp, char const *type)
{
	fr_quickheap_t	*qh;

	if (!cmp) return NULL;

	qh = talloc_zero(ctx, fr_quickheap_t);
	if (!qh) return NULL;

	qh->heap = talloc_array(qh, void *, INITIAL_CAPACITY);
	if (!qh->heap) {
cleanup:
		talloc_free(qh);
		return NULL;
	}

	qh->s = stack_alloc(qh, size_t);
	if (!qh->s) goto cleanup;

	qh->capacity = INITIAL_CAPACITY + 1;
	stack_push(qh->s, 0);
	qh->idx = 0;

	qh->rand_ctx.a = fr_rand();
	qh->rand_ctx.b = fr_rand();
	qh->type = type;
	qh->cmp = cmp;

	return qh;
}

/*
 * todo:
 *	fr_quickheap_{peek, pop}(): return NULL if quickheap is empty
 *	fr_quickheap_insert(): attempt to extend the heap if it's full
 */

void * fr_quickheap_peek(fr_quickheap_t *qh)
{
	incremental_quicksort(qh->heap, qh->idx, qh->s);
	return heap_sub(qh, qh->idx);
}

void * fr_quickheap_pop(fr_quickheap_t *qh)
{
	incremental_quicksort(qh->heap, qh->idx, qh->s);
	qh->idx++;
	stack_pop(qh->s);
	return heap_sub(qh, qh->idx - 1);
}

void fr_quickheap_insert(fr_quickheap_t *qh, void *data)
{
	for (size_t pidx = 0; ; pidx++) {
		size_t	pivot = stack_item(qh->s, pidx));
		heap_sub(qh, pivot + 1) = heap_sub(qh, pivot);
		stack_set(qh->s, pidx, pivot + 1);
		if (stack_depth(qh->s) == pivot + 1 || 
		    qh->cmp(heap_sub(stack_item(qh->s, pidx + 1), data) <= 0) break;
		heap_sub(qh, pivot - 1) = heap_sub(qh, stack_item(qh->s, pidx + 1) + 1);
	}

	heap_sub(qh, stack_item(qh->s, pidx) - 1) = data;
}

/*
 * Quickheap is built around "incremental quicksort"; it doesn't sort
 * a whole array, but only the first few elements.
 *
 * This is almost the version from the paper; unlike it, it "returns" void
 * because none of its callers actually use the value the paper version
 * returns. Also, the comments on the quickheap operations say the actual
 * version omits the stack pop the paper version does just before returning.
 */
static void incremental_quicksort(void **a, size_t idx, stack_t s, fr_heap_cmp_t cmp)
{
	while (idx != stack_top(s)) {
		size_t	pidx, pidx_prime;

		pidx = /* random number between idx and stack_top(s) - 1, inclusive */
		pidx_prime = partition(a, a[pidx], idx, stack_top(s) - 1, cmp);
		stack_push(s, pidx_prime);
	}
}

static void incremental_quicksort(fr_quickheap_t *qh)
{
	while (qh->idx != stack_top(qh->s)) {
		size_t pidx = qh->idx + fr_fast_rand(qh->rand_ctx) % (stack_top(qh->s) - qh->idx);
		size_t pidx_prime = partition(qh, heap_sub(qh, pidx), idx, stack_top(qh->s) - 1);

		stack_push(qh->s, pidx_prime);
	}
}

/*
 * partition permutes qh's heap from low to high (modulo its circular nature)
 * so that when the smoke clears, nothing from low up to the pivot's position
 * (which may have to change) "follows" the pivot and nothing from the pivot's
 * position to high "precedes" it.
 */
static size_t partition(fr_quickheap_t *qh, void *pivot, size_t low, size_t high)
{
	size_t	l = low - 1;
	size_t	h = high + 1;
	void	*temp;

	for (;;) {
		while (qh->cmp(heap_sub(qh, ++l), pivot) < 0) ;
		while (qh->cmp(heap_sub(qh, --h), pivot) > 0) ;
		if (l >= h) break;
		temp = heap_sub(qh, l);
		heap_sub(qh, l) = heap_sub(qh, h);
		heap_sub(qh, h) = temp;
	}
	return h;
}


