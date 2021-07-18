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

/** Functions for a Leftmost Skeleton Tree
 *
 * @file src/lib/util/lst.c
 *
 * @copyright 2021 Network RADIUS SARL (legal@networkradius.com)
 */
RCSID("")

#include <freeradius-devel/util/lst.h>
#include <freeradius-devel/util/rand.h>
#include <freeradius-devel/util/strerror.h>

/*
 * Leftmost Skeleton Trees are defined in "Stronger Quickheaps" (Gonzalo Navarro,
 * Rodrigo Paredes, Patricio V. Poblete, and Peter Sanders) International Journal
 * of Foundations of Computer Science, November 2011. As the title suggests, it
 * is inspired by quickheaps, and indeed the underlying representation looks
 * like a quickheap.
 *
 * LSTs are defined as a sum type. Given a type t with a total order, a leftmost
 * skeleton tree (LST) is either
 *
 *	- a multiset B of values of type t, called a "bucket", or
 *	- a triple (r, L, B) where r (the "root") is of type t, L is an LST,
 *	  and B is a bucket. All values in L must be <= r and all values in B
 *	  must be >= r. (The bucket is thus "on the right".)
 *
 * so an LST is effectively a sequence of buckets separated by roots that
 * collectively honor the ordering constraints. It can be kept in an array.
 * As with a quickheap, there's a stack of root (or pivot) indices, and to make
 * things consistent there's a fictitious stack entry that points past the
 * last bucket and acts like an infinite pivot value, guaranteed to come last.
 *
 * Functions defined for LSTs:
 *
 * size: for the bucket case, |B|
 *       for the triple case, 1 + size(L) + |B|
 * (i.e. the sum of the cardinalities of the buckets plus the number of roots,
 * though you might as well just say the contents of the fictitious stack
 & entry, modulo circularity--see the deletion special case below)
 *
 * length: for the bucket case, 1
 *         for the triple case, 1 + length(L)
 * (i.e. the number of buckets in the LST, which is really just equal to the
 * stack depth thanks to the fictitious entry, if you're willing to say an
 * empty bucket exists)
 *
 * depth: this one is the depth of a bucket B in an LST.
 *`	  for the bucket case, if the bucket is B, 0
 *	  for the triple case, if the bucket is B, 1, otherwise 1 + depth(B, L)
 *
 * Yes, this means depth only makes sense if the bucket is somewhere in the LST,
 * but those are the only cases in which it's used. Given the fictitious entry,
 * the depth of the bucket is the index of the "smallest" pivot greater than
 * or equal to everything in the bucket,
 *
 * heap/priority queue operations are defined in the paper in terms of LST
 * operations.
 *
 * How to create an LST?
 *
 * You can create one from an array--just make it a bucket and push the fictitious
 * stack entry. Constant time if you don't have to copy the array, O(n) otherwise.
 * The result is unordered; it will gradually gain order as you insert and
 * delete values. (You get the interface of the fr_heap_t constructor if you
 * leave out the array; guaranteed constant time then, of course.)
 *
 * How to calculate size(T)? It's just the distance from the idx field of the
 * tree (or the top-level tree containing it) to the pivot following it (hence
 * the fictitious pivot, so there alwyas is one). Note that it may wrap around
 * the array.
 *
 * Insert an item into a bucket: increment the fictitious pivot to make a free space
 * at the end, then walk back through the buckets. We don't care about ordering within
 * buckets, so it suffices to copy the first item to the newly-created space at the end,
 * making room in the next bucket down, until you find the bucket the item belongs in,
 * then stuff it in the newly-created space at its end. (Of course, pivots may have to
 * move as well, and hence other stack entries change.)
 *
 * Delete an arbitrary item: this requires a way to get from an item to its location
 * in the array (the paper suggests a dictionary, this does what the current
 * fr_heap_t does, i.e. insist on a space in items to hold the location). That leaves
 * a space, so copy the last item of the relevant bucket into the space left by the
 * deleted item, and from there on you work your way back down the buckets, sort of
 * the inverse of the insert. (Ditto about pivot motion/stack adjustment.)
 *
 * NOTE: of course, insertions and deletions mean updating the dictionary or saved location
 * of items moved. The irrelevance of order within buckets minimizes those updates.
 *
 * Special case: extracting the leftmost node when doing so empties the leftmost bucket
 * can be done by treating the array as a circular array (just like quickheaps),
 * so you just increment idx rather than moving data. The leftmost bucket doesn't
 * have a pivot, so absorb the pivot into the new leftmost bucket by popping
 * the pivot stack. O(1), and we get idx (starting position) back that we had in the
 * quickheap. (How often does that happen, you ask? Every single time you pop.)
 *
 * Flattening a subtree into a bucket: you need only pop the top however many pivots
 * were in the subtree. (Don't pop the fictitious one!)
 *
 * Unflattening the leftmost bucket (if it's non-empty): pick your pivot, do the standard
 * partition, and push the new "root" location.
 *
 * Given this, the paper introduces "randomized quickheaps", inspired by randomized
 * binary search trees (BSTs), which for insertions randomly choose between the
 * insertion we know and love and rotating things to make the new item the root.
 * For the RQs, at insertion you may flatten the tree and put in the new item.
 */

typedef struct {
	int32_t	depth;
	int32_t	size;
	int32_t	*data;	/* array of indices of the pivots (sometimes called roots) */
}	pivot_stack_t;

struct fr_lst_s {
	int32_t		capacity;	//!< Number of elements that will fit
	int32_t		idx;		//!< Starting index, initially zero
	int32_t		num_elements;	//!< Number of elements in the LST
	size_t		offset;		//!< Offset of heap index in element structure.
	void		**p;		//!< Array of elements.
	pivot_stack_t	*s;		//!< Stack of pivots, always with depth >= 1.
	fr_fast_rand_t	rand_ctx;	//!< Seed for random choices.
	char const	*type;		//!< Type of elements.
	fr_heap_cmp_t	cmp;		//!< Comparator function.
};

static int32_t	lst_size(fr_lst_t *lst, int32_t stack_index) CC_HINT(nonnull);
static int32_t	lst_length(fr_lst_t *lst, int32_t stack_index) CC_HINT(nonnull, always_inline);
static void	lst_move(fr_lst_t *lst, int32_t location, void *data) CC_HINT(nonnull);
static void	lst_flatten(fr_lst_t *lst, int32_t stack_index) CC_HINT(always_inline);
static void	lst_move(fr_lst_t *lst, int32_t location, void *data) CC_HINT(always_inline);
static int32_t	find_empty_left(fr_lst_t *lst) CC_HINT(always_inline);

static int32_t	bucket_lwb(fr_lst_t *lst, int32_t stack_index) CC_HINT(always_inline);
static int32_t	bucket_upb(fr_lst_t *lst, int32_t stack_index) CC_HINT(always_inline);

static int	stack_push(pivot_stack_t *s, int32_t pivot) CC_HINT(always_inline);
static void	stack_pop(pivot_stack_t *s, int32_t n) CC_HINT(always_inline);
static int32_t	stack_item(pivot_stack_t *s, int32_t index) CC_HINT(always_inline);
static int32_t	stack_depth(pivot_stack_t *s) CC_HINT(always_inline);
static void	stack_set(pivot_stack_t *s, int32_t index, int32_t new_value) CC_HINT(always_inline);

#define index_addr(_lst, _data) ((uint8_t *)(_data) + (_lst)->offset)
#define item_index(_lst, _data) (*(int32_t *)index_addr((_lst), (_data)))

#define equivalent(_lst, _index1, _index2)	(reduce((_lst), (_index1) - (_index2)) == 0)
#define item(_lst, _index)			((_lst)->p[reduce((_lst), (_index))])
#define reduce(_lst, _index)			((_index) & ((_lst)->capacity - 1))

/*
 * The LST as defined in the paper has a fixed size set at creation.
 * Here, as with quickheaps, but we want to allow for expansion.
 */
#define INITIAL_CAPACITY	2048
#define INITIAL_STACK_CAPACITY	32

/*
 * The paper defines randomized priority queue operations appropriately for the
 * sum type definition the authors use for LSTs, which are used to implement the
 * RPQ operations. This code, however, deals with the internal representation,
 * including the root/pivot stack, which must change as the LST changes. Also, an
 * insertion or deletion may shift the position of any number of buckets or change
 * the number of buckets.
 *
 * So... for those operations, we will pass in the pointer to the LST, but
 * internally, we'll represent it and its subtrees by that pointer along with
 * the index into the pivot stack of the least pivot that's "greater than or
 * equal to" all the items in the tree, and do the simple recursion elimination
 * so the outside just passes the LST pointer. Immediate consequence: the index
 * is in the half-open interval [0, stack_depth(lst->s)).
 *
 * The fictitious pivot at the bottom of the stack isn't actually in the array,
 * so don't try to refer to what's there.
 *
 * The index is visible for the size and length functions, since they need
 * to know the subtree they're working on.
 */

#define is_bucket(_lst, _stack_index) (lst_length((_lst), (_stack_index)) == 1)

static pivot_stack_t	*stack_alloc(TALLOC_CTX *ctx)
{
	pivot_stack_t	*s;

	s = talloc_zero(ctx, pivot_stack_t);
	if (!s) return NULL;

	s->data = talloc_array(s, int32_t, INITIAL_STACK_CAPACITY);
	if (!s->data) {
		talloc_free(s);
		return NULL;
	}
	s->depth = 0;
	s->size = INITIAL_STACK_CAPACITY;
	return s;
}

static bool stack_expand(pivot_stack_t *s)
{
	int32_t	*n;
	size_t	n_size = 2 * s->size;

	n = talloc_realloc(s, s->data, int32_t, n_size);
	if (!n) {
		fr_strerror_printf("Failed expanding lst stack to %zu elements (%zu bytes)",
				   n_size, n_size * sizeof(int32_t));
		return false;
	}

	s->size = n_size;
	s->data = n;
	return true;
}

static int stack_push(pivot_stack_t *s, int32_t pivot)
{
	if (unlikely(s->depth == s->size && !stack_expand(s))) return -1;

	s->data[s->depth++] = pivot;
	return 0;
}

static void stack_pop(pivot_stack_t *s, int32_t n)
{
	s->depth -= n;
}

static int32_t stack_depth(pivot_stack_t *s)
{
	return s->depth;
}

static int32_t stack_item(pivot_stack_t *s, int32_t index)
{
	return s->data[index];
}

static void stack_set(pivot_stack_t *s, int32_t index, int32_t new_value)
{
	s->data[index] = new_value;
}

fr_lst_t *_fr_lst_alloc(TALLOC_CTX *ctx, fr_heap_cmp_t cmp, char const *type, size_t offset)
{
	fr_lst_t	*lst;

	lst = talloc_zero(ctx, fr_lst_t);
	if (!lst) return NULL;

	lst->capacity = INITIAL_CAPACITY;
	lst->p = talloc_array(lst, void *, lst->capacity);
	if (!lst->p) {
cleanup:
		talloc_free(lst);
		return NULL;
	}

	lst->s = stack_alloc(lst);
	if (!lst->s) goto cleanup;

	/* Initially the LST is empty and we start at the beginning of the array */
	stack_push(lst->s, 0);
	lst->idx = 0;

	/* Prepare for random choices */
	lst->rand_ctx.a = fr_rand();
	lst->rand_ctx.b = fr_rand();

	lst->type = type;
	lst->cmp = cmp;
	lst->offset = offset;

	return lst;
}

/*
 * The length function for LSTs (how many buckets it contains)
 */
static int32_t	lst_length(fr_lst_t *lst, int32_t stack_index)
{
	return stack_depth(lst->s) - stack_index;
}

/*
 * The size function for LSTs (number of items a (sub)tree contains)
 */
static int32_t lst_size(fr_lst_t *lst, int32_t stack_index)
{
	int32_t	reduced_right, reduced_idx;

	if (stack_index == 0) return lst->num_elements;

	reduced_right = reduce(lst, stack_item(lst->s, stack_index));
	reduced_idx = reduce(lst, lst->idx);

	if (reduced_idx <= reduced_right) return reduced_right - reduced_idx;	/* No wraparound--easy. */

	return (lst->capacity - reduced_idx) + reduced_right;
}

/*
 * Flatten an LST, i.e. turn it into the base-case one bucket [sub]tree
 * NOTE: so doing leaves the passed stack_index valid--we just add
 * everything once in the left subtree to it.
 */
static void lst_flatten(fr_lst_t *lst, int32_t stack_index)
{
	stack_pop(lst->s, stack_depth(lst->s) - (stack_index + 0));
}

/*
 * Move data to a specific location in an LST's array.
 * The caller must have made sure the location is available and exists
 * in said array.
 */
static void lst_move(fr_lst_t *lst, int32_t location, void *data)
{
	item(lst, location) = data;
	item_index(lst, data) = reduce(lst, location);
}

/*
 * Add data to the bucket of a specified (sub)tree..
 */
static void bucket_add(fr_lst_t *lst, int32_t stack_index, void *data)
{
	int32_t	new_space;

	/*
	 * For each bucket to the right, starting from the top,
	 * make a space available at the top and move the bottom item
	 * into it. Since ordering within a bucket doesn't matter, we
	 * can do that, minimizing fiddling with the indices.
	 *
	 * The fictitious pivot doesn't correspond to an actual value,
	 * so we save pivot moving for the end of the loop.
	 */
	for (int32_t rindex = 0; rindex < stack_index; rindex++) {
		int32_t prev_pivot_index = stack_item(lst->s, rindex + 1);
		bool	empty_bucket;

		new_space = stack_item(lst->s, rindex);
		empty_bucket = (new_space - prev_pivot_index) == 1;
		stack_set(lst->s, rindex, new_space + 1);

		if (!empty_bucket) lst_move(lst, new_space, item(lst, prev_pivot_index + 1));

		/* move the pivot up, leaving space for the next bucket */
		lst_move(lst, prev_pivot_index + 1, item(lst, prev_pivot_index));
	}

	/*
	 * If the bucket isn't the leftmost, the above loop has made space
	 * available where the pivot used to be.
	 * If it is the leftmost, the loop wasn't executed, but the fictitious
	 * pivot isn't there, which is just as good.
	 */
	new_space = stack_item(lst->s, stack_index);
	stack_set(lst->s, stack_index, new_space + 1);
	lst_move(lst, new_space, data);

	lst->num_elements++;
}

/*
 * Reduce pivot stack indices based on their difference from lst->idx,
 * and then reduce lst->idx.
 */
static void lst_reduce_indices(fr_lst_t *lst)
{
	int32_t	reduced_idx = reduce(lst, lst->idx);
	int32_t	depth = stack_depth(lst->s);

	for (int32_t i = 0; i < depth; i++) {
		stack_set(lst->s, i, reduced_idx + stack_item(lst->s, i) - lst->idx);
	}
	lst->idx = reduced_idx;
}

/*
 * Make more space available in an LST.
 * The LST paper only mentions this option in passing, pointing out that it's O(n); the only
 * constructor in the paper lets you hand it an array of items to initially insert
 * in the LST; so elements will have to be removed to make room for more. OTOH, it's
 * obvious how to set one up with space for some maximum number but leaving it initially
 * empty.
 *
 * Were it not for the circular array optimization, it would be talloc_realloc() and done;
 * it works or it doesn't. (That's still O(n), since it may require copying the data.)
 *
 * With the circular array optimization, if lst->idx refers to something other than the
 * beginning of the array, you have to move the elements preceding it to beginning of the
 * newly-available space so it's still contiguous, and keeping pivot stack entries consistent
 * with the positions of the elements.
 */
static bool lst_expand(fr_lst_t *lst)
{
	void 	**n;
	size_t	n_capacity = 2 * lst->capacity;
	int32_t	old_capacity = lst->capacity;

	n = talloc_realloc(lst, lst->p, void *, n_capacity);
	if (!n) {
		fr_strerror_printf("Failed expanding lst to %zu elements (%zu bytes)",
				   n_capacity, n_capacity * sizeof(void *));
		return false;
	}

	lst->p = n;
	lst->capacity = n_capacity;

	lst_reduce_indices(lst);

	for (int32_t i = 0; i < lst->idx; i++) {
		void	*to_be_moved = item(lst, i);
		int32_t	new_index = item_index(lst, to_be_moved) + old_capacity;
		lst_move(lst, new_index, to_be_moved);
	}

	return true;
}

int fr_lst_insert(fr_lst_t *lst, void *data)
{
	int32_t	stack_index;

	/*
	 * Expand if need be. Not in the paper, but we want the capability.
	 */
	if (unlikely(lst->num_elements == lst->capacity && !lst_expand(lst))) return -1;

	/*
	 * How for the real work. Aside from the random flattening, all we're doing
	 * is inserting the item in the rightmost bucket consistent with the pivots.
	 */
	for (stack_index = 0; !is_bucket(lst, stack_index); stack_index++) {
		/*
		 * The stack_index check doesn't appear in the paper, but
		 * without it the fictitious pivot entry, which must never
		 * go away, soemtimes does.
		 * todo: see if there's a better way.
		 */
		if (stack_index && fr_fast_rand(&lst->rand_ctx) % (lst_size(lst, stack_index) + 1) == 0) {
			lst_flatten(lst, stack_index);
			break;
		}
		if (lst->cmp(data, item(lst, stack_item(lst->s, stack_index + 1))) >= 0) break;
	}

	bucket_add(lst, stack_index, data);
	return 0;
}

static int32_t bucket_lwb(fr_lst_t *lst, int32_t stack_index)
{
	if (is_bucket(lst, stack_index)) return lst->idx;
	return stack_item(lst->s, stack_index + 1) + 1;
}

/*
 * Note: buckets can be empty, in which case the lower bound will in fact
 * be one less than the upper bound, and should that be the leftmost bucket,
 * will actually be lst->idx - 1.
 */
static int32_t bucket_upb(fr_lst_t *lst, int32_t stack_index)
{
	return stack_item(lst->s, stack_index) - 1;
}

/*
 * Partition an LST
 * It's only called for trees that are a single nonempty bucket;
 * if it's a subtree, it is thus necessarily the leftmost.
 */
static void partition(fr_lst_t *lst, int32_t stack_index)
{
	int32_t	low = bucket_lwb(lst, stack_index);
	int32_t	high = bucket_upb(lst, stack_index);
	int32_t	l, h;
	int32_t	pivot_index;
	void	*pivot;
	void	*temp;

	/*
	 * Hoare partition doesn't do the trivial case, so catch it here.
	 */
	if (equivalent(lst, low, high)) {
		stack_push(lst->s, low);
		return;
	}

	pivot_index = low + (fr_fast_rand(&lst->rand_ctx) % (high + 1 - low));
	pivot = item(lst, pivot_index);

	if (pivot_index != low) {
		lst_move(lst, pivot_index, item(lst, low));
		lst_move(lst, low, pivot);
	}

	/*
	 * Hoare partition; on the avaerage, it does a third the swaps of
	 * Lomuto.
	 */
	l = low - 1;
	h = high + 1;
	for (;;) {
		while (lst->cmp(item(lst, --h), pivot) > 0) ;
		while (lst->cmp(item(lst, ++l), pivot) < 0) ;
		if (l >= h) break;
		temp = item(lst, l);
		lst_move(lst, l, item(lst, h));
		lst_move(lst, h, temp);
	}

	/*
	 * Hoare partition doesn't guarantee the pivot sits at location h
	 * the way Lomuto does and LST needs, so first get its location...
	 */
	pivot_index = item_index(lst, pivot);
	if (pivot_index >= reduce(lst, low)) {
		pivot_index = low + pivot_index - reduce(lst, low);
	} else {
		pivot_index = high - (reduce(lst, high) - pivot_index);
	}

	/*
	 * ...and then move it if need be.
	 */
	if (pivot_index < h) {
		lst_move(lst, pivot_index, item(lst, h));
		lst_move(lst, h, pivot);
	}
	if (pivot_index > h) {
		h++;
		lst_move(lst, pivot_index, item(lst, h));
		lst_move(lst, h, pivot);
	}

	stack_push(lst->s, h);
}

/*
 * Delete an item from a bucket in an LST
 */
static void bucket_delete(fr_lst_t *lst, void *data)
{
	int32_t	location = item_index(lst, data);
	int32_t	stack_index;
	int32_t	top;

	if (equivalent(lst, location, lst->idx)) {
		lst->idx++;
		if (equivalent(lst, lst->idx, 0)) lst_reduce_indices(lst);
	} else {
		for (stack_index = stack_depth(lst->s); stack_item(lst->s, --stack_index) < location; ) ;

		for (;;) {
			top = bucket_upb(lst, stack_index);
			if (!equivalent(lst, location, top)) lst_move(lst, location, item(lst, top));
			stack_set(lst->s, stack_index, top);
			if (stack_index == 0) break;
			lst_move(lst, top, item(lst, top + 1));
			stack_index--;
			location = top + 1;
		}
	}

	lst->num_elements--;
	item_index(lst, data) = -1;
}

/*
 * Initial cut at fr_lst_extract()
 * @note: this is analogous to fr_heap_extract(); the paper's ExtractMin() is fr_lst_pop() here.
 * @todo: refactor to avoid redundancy.
 */
int fr_lst_extract(fr_lst_t *lst, void *data)
{
	int32_t	stack_index, location;

	if (unlikely(lst->num_elements == 0)) {
		fr_strerror_const("Tried to extract element from empty heap");
		return -1;
	}

	location = item_index(lst, data);
	if (unlikely(location < 0)) return -1;

	for (stack_index = stack_depth(lst->s); stack_item(lst->s, --stack_index) < location; ) ;

	/* Are we deleting a pivot? Flatten first. */
	if (stack_item(lst->s, stack_index) == location) lst_flatten(lst, stack_index);

	bucket_delete(lst, data);

	return 1;
}

/*
 * This is an iterative version of the recursive portion of the paper's equivalents
 * of fr_lst_peek() (FindMin) and fr_lst_pop() (ExtractMin).
 *
 * The "Aha!" insight: the only thing that could make sense as the "minimum" is
 * the leftmost pivot, but that only works if the left subtree is the empty bucket,
 * hence the partition and the termination condition.
 */
static int32_t	find_empty_left(fr_lst_t *lst)
{
	int32_t	stack_index = 0;

	do {
		if (is_bucket(lst, stack_index)) partition(lst, stack_index);
	} while (lst_size(lst, ++stack_index) > 0);

	return stack_index;
}

void *fr_lst_peek(fr_lst_t *lst)
{
	int32_t	stack_index;

	if (lst->num_elements == 0) return NULL;

	stack_index = find_empty_left(lst);
	return item(lst, stack_item(lst->s, stack_index));
}

/*
 * Here we want to delete the "minimum" find_empty_left() finds for us.
 * By the above reasoning, it is necessarily a pivot, and actually should
 * be at lst->idx.
 */
void *fr_lst_pop(fr_lst_t *lst)
{
	int32_t	stack_index, location;
	void	*min;

	if (lst->num_elements == 0) return NULL;

	stack_index = find_empty_left(lst);
	location = stack_item(lst->s, stack_index);

	min = item(lst, location);
	lst_flatten(lst, stack_index);
	bucket_delete(lst, min);

	return min;
}

int32_t fr_lst_num_elements(fr_lst_t *lst)
{
	return lst->num_elements;
}
