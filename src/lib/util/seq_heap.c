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

/** Functions for a sequence heap
 *
 * @file src/lib/util/seq_heap.c
 *
 * @copyright 2021 The FreeRADIUS server project
 */
RCSID("")

#include <freeradius-devel/util/seq_heap.h>
#include <freeradius-devel/util/heap.h>

/*
 *	A sequence heap ("Fast Priority Queues for Cached Memory", Peter Sanders,
 *	ACM Journal of Experimental Algorithmics, December 2000) implements heap
 *	functions (insert, and retrieve the "minimum") in a way intended
 *	to avoid cache misses.
 *
 *	TODO: implement peek() at least, and determine whether it makes sense to
 *	implement an iterator.
 *
 *	It does this by trying to keep most of the action in the insertion buffer
 *	(a heap) and deletion buffer, which collectively fit within level one cache. When
 *	the insertion buffer fills, items are moved so that the deletion buffer
 *	holds the next items to remove and the rest go into one or more "groups".
 *
 *	Sequence heaps are parametrized by some values that are a function of the
 *	level-one cache size and the size of the blocks that move between cache
 *	and external memory, called "M" and "B" respectively in Sanders. The values,
 *	again using Sanders nomenclature:
 *
 *	k  the number of sequences per merge group (and the ratio between sizes of
 *	   sequences in successive groups); proportional to M/B.
 *	m  the maximum number of items in the insertion buffer (and the size of
 *	   the merge group buffers and the sequences in the first (zeroth?) group);
 *	   proportional to M, but m * the size of each item can't exceed M.
 *	   (Sanders's first use of m looks like it could be a size in bytes, but later
 *	   it's clearly used to refer to a number of elements.)
 *	m' the maximum number of elements in the deletion buffer; much less than m
 *
 *	Ultimately they should be based on the actual values of M and B on the
 *	target processor. Now they match the values used in the tests in Sanders.
 *
 *	The data here are scattered across the insertion buffer, deletion
 *	buffer, group buffers, and group sequences. Can we implement an iterator
 *	for a sequence heap? Yes, if users don't count on order and nobody
 *	inserts or deletes in mid-iteration.
 */

#define SEQ_PER_GROUP		128	/* k */
#define INS_BUFFER_SIZE		256	/* m */
#define DEL_GUFFER_SIZE		 32	/* m' */
#define GRP_BUFFER_SIZE		INS_BUFFER_SIZE

typedef struct seq_heap_group_s	seq_heap_group_t;

/*
 * Merge group i (we will start with zero, C style, rather than one as Sanders
 * does) has:
 *    - k sequences of at most mk^i items; items go here when the
 *      insertion buffer fills or when merge group (i - 1) fills up (and
 *	there's room here)
 *    - a group buffer that the sequences get merged into; the merge groups'
 *      buffers are in turn merged into the deletion buffer
 */
struct seq_heap_group_s {
	void	*seq[SEQ_PER_GROUP];
	void	*buffer[GRP_BUFFER_SIZE];
};

struct fr_seq_heap_s {
	size_t			num_groups;
	seq_heap_group_t	*groups;
	heap_t			*insertion_buffer;
	void			**deletion_buffer;
	fr_heap_cmp_t		cmp;
	char const		*type;
};

fr_seq_heap_t *_fr_seq_heap_alloc(TALLOC_CTX *ctx, fr_heap_cmp_t cmp, char const *type)
{
	fr_seq_heap_t	*fsh;

	if (!cmp) return NULL;

	fsh = talloc_zero(ctx, fr_seq_heap_t);
	if (!fsh) return NULL;

	/*
	 *	set up insertion buffer and deletion buffer
	 */

	fsh->type = type;
	fsh->cmp = cmp;

	return fsh;
}

static int8_t	cmp_wrapper(fr_heap_cmp_t *cmp, void *data1, void *data2)
{
	if (data1 == NULL) return 1;
	if (data2 == NULL) return -1;

	return cmp(data1, data2);
}

/*
 * If the sequence heap isn't empty, remove and return the "minimum" element.
 *
 * To work, this requires
 * 1. Nothing io any group buffer "precedes" anything in the deletion buffer.
 * 2. For each group G, nothing in G's sequences "precedes" anything in G's buffer.
 *
 * Clearly this function will preserve those properties.
 * (Also, the number of elements in the insertion buffer either stays the same
 * or decreases by one, so
 */
void *fr_seq_heap_pop(fr_seq_heap_t *shp)
{
	void	*insertion_data = /* peek at shp->insertion_buffer */;
	void	*deletion_data = /* peek at shp->deletion_buffer */;

	if (cmp_wrapper(shp->cmp, insertion_data, deletion_data) < 0) {
		/* remove top of shp->insertion_buffer */
		return insertion_data;
	}

	/* remove top of shp->deletion_buffer */
	if (/* shp->deletion_buffer is empty */) {
		/* refill each non-empty group's group buffer that has < m' items */
		/* refill shp->deletion_buffer from group buffers */
	}
	return deletion_data;
}

/** Insert a new element into the sequence heap
 *
 * @param[in] shp	The sequence heap to insert an element into.
 * @param[in] data	Data to insert into the heap.
 * @return
 *	- 0 on success.
 *	- -1 on failure .
 */
int fr_seq_heap_insert(fr_heap_t *shp, void *data)
{
	void *[INS_BUFFER_SIZE + GRP_BUFFER_SIZE + DEL_BUFFER_SIZE];

	if (/* insertion of data into shp->insertion_buffer fails */) return -1;

	if (/* shp->insertion_buffer holds < m items */) return 0;

	/*
	 * Generate a sorted sequence from insertion buffer, deletion buffer, and
	 * the first group's group buffer if it exists (m <= total length <= 2m + m').
	 * Put the first (up to) m' in the deletion buffer.
	 * If anything's left, put the next (up to) m in the first group's group buffer.
	 *
	 * If nothing's left, you're done.
	 * Otherwise, there's at most m items left.
	 * If there's an unused sequence in the first group, put it there.
	 *
	 *   If there's an unused sequence in the first group, put it there.
	 *   If there's not room, merge the first group sequences and try to move them
	 *   to the next one; that may walk further through the groups and even
	 *   require adding one.
	 *
	 * NOTE: that movement from one group to the next can break precondition (2)
	 * of fr_seq_heap_pop(), so if G1 through Gi are emptied, merge G1 through G(i+1)'s
	 * group buffers and put them in G1. (That implies that there better not be
	 * more than k groups, but given the exponential growth of successive group
	 * sequence lengths, that would seem highly unlikely.)
	 */

	 return 0;
}

