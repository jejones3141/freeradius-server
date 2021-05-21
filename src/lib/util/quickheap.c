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
 * @copyright 2021 Network RADIUS SARL (legal@networkradius.com)
 */
RCSID("")

#include <freeradius-devel/util/quickheap.h>

#include <freeradius-devel/util/rand.h>

/*
 * "Quickheaps: Simple, Efficient, and Cache-Oblivious" by Gonzalo Navarro and
 * Rodrigo Paredes defines the quickheap structure. It's nearly as simple as
 * the binary heap, but more cache-friendly.
 */

/*
 * The quickheap uses a stack of pivot indices.
 */
typedef struct {
	uint32_t	depth;
	uint32_t	size;
	uint32_t	*data;
}	pivot_stack_t;

struct fr_quickheap_s {
	uint32_t	capacity;
	uint32_t	idx;
	void		**heap;
	pivot_stack_t	*s;
	fr_fast_rand_t	rand_ctx;	/* for random choice of pivot in incremental_quicksort */
	char const	*type;
	fr_heap_cmp_t	cmp;
};

static void incremental_quicksort(fr_quickheap_t *qh);
static size_t partition(fr_quickheap_t *qh, void *pivot, size_t low, size_t high);

static pivot_stack_t *stack_alloc(TALLOC_CTX *ctx);

/*
 * The quickheap as defined in the paper have a fixed size set at creation.
 * We will ultimately let them grow as the current fr_heap_t can.
 */
#define INITIAL_CAPACITY	2048

#define INITIAL_STACK_CAPACITY	32

/*
 * The quickheap heap is treated as a circular array, hence this macro.
 * It should be usable where an lvalue is required.
 */
#define heap_sub(_qh, _n)	((_qh)->heap[(_n) % (_qh)->capacity])

static pivot_stack_t	*stack_alloc(TALLOC_CTX *ctx)
{
	pivot_stack_t	*s;

	s = talloc_zero(ctx, pivot_stack_t);
	if (!s) return NULL;

	s->data = talloc_array(s, uint32_t, INITIAL_STACK_CAPACITY);
	if (!s->data) {
		talloc_free(s);
		return NULL;
	}
	s->depth = 0;
	s->size = INITIAL_STACK_CAPACITY;
	return s;
}

static void stack_push(pivot_stack_t *s, uint32_t pivot)
{
	/* todo: try to extend if it's full */
	s->data[s->depth++] = pivot;
}

static uint32_t stack_top(pivot_stack_t *s)
{
	/* */
	return s->data[s->depth - 1];
}

static void stack_pop(pivot_stack_t *s)
{
	s->depth--;
}

static uint32_t stack_depth(pivot_stack_t *s)
{
	return s->depth;
}

static uint32_t stack_item(pivot_stack_t *s, uint32_t index)
{
	return s->data[index];
}

static void stack_set(pivot_stack_t *s, uint32_t index, uint32_t new_value)
{
	s->data[index] = new_value;
}

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

	qh->s = stack_alloc(qh);
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
	incremental_quicksort(qh);
	return heap_sub(qh, qh->idx);
}

void * fr_quickheap_pop(fr_quickheap_t *qh)
{
	incremental_quicksort(qh);
	qh->idx++;
	stack_pop(qh->s);
	return heap_sub(qh, qh->idx - 1);
}

void fr_quickheap_insert(fr_quickheap_t *qh, void *data)
{
	uint32_t pidx = 0;

	for (;;) {
		uint32_t	pivot = stack_item(qh->s, pidx);

		heap_sub(qh, pivot + 1) = heap_sub(qh, pivot);
		stack_set(qh->s, pidx, pivot + 1);
		if (stack_depth(qh->s) == pivot + 1 ||
		    qh->cmp(heap_sub(qh, stack_item(qh->s, pidx + 1)), data) <= 0) break;
		heap_sub(qh, pivot - 1) = heap_sub(qh, stack_item(qh->s, pidx + 1) + 1);
		pidx++;
	}

	heap_sub(qh, stack_item(qh->s, pidx) - 1) = data;
}

uint32_t fr_quickheap_num_elements(fr_quickheap_t *qh)
{
	uint32_t	heap_end = stack_item(qh->s, 0);

	if (heap_end >= qh->idx) return heap_end - qh->idx;
	return qh->capacity - (qh->idx - heap_end);
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

static void incremental_quicksort(fr_quickheap_t *qh)
{
	while (qh->idx != stack_top(qh->s)) {
		size_t pidx = qh->idx + fr_fast_rand(&qh->rand_ctx) % (stack_top(qh->s) - qh->idx);
		size_t pidx_prime = partition(qh, heap_sub(qh, pidx), qh->idx, stack_top(qh->s) - 1);

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
	uint32_t	l = low - 1;
	uint32_t	h = high + 1;
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


