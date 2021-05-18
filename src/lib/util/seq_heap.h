#pragma once
/*
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 */

/** Structures and prototypes for sequence heaps
 *
 * @file src/lib/util/seq_heap.h
 *
 * @copyright 2021 The FreeRADIUS server project
 */
RCSIDH(seq_heap_h, "")

typedef struct fr_seq_heap_s fr_seq_heap_t;

/** Creates a sequence heap that can be used with non-talloced elements
 *
 * @param[in] _ctx		Talloc ctx to allocate heap in.
 * @param[in] _cmp		Comparator used to compare elements.
 * @param[in] _type		Of elements.
 */
#define fr_seq_heap_alloc(_ctx, _cmp, _type) \
	_fr_seq_heap_alloc(_ctx, _cmp, NULL)

/** Creates a sequence heap that verifies elements are of a specific talloc type
 *
 * @param[in] _ctx		Talloc ctx to allocate heap in.
 * @param[in] _cmp		Comparator used to compare elements.
 * @param[in] _talloc_type	of elements.
 * @return
 *	- A new heap.
 *	- NULL on error.
 */
#define fr_seq_heap_talloc_alloc(_ctx, _cmp, _talloc_type) \
	_fr_seq_heap_alloc(_ctx, _cmp, #_talloc_type)

