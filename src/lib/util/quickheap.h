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

/** Structures and prototypes for quickheaps
 *
 * @file src/lib/util/quickheap.h
 *
 * @copyright 2021  Network RADIUS SARL (legal@networkradius.com)
 */
RCSIDH(quickheap_h, "")

#ifdef __cplusplus
extern "C" {
#endif

#include <freeradius-devel/build.h>
#include <freeradius-devel/util/talloc.h>

#include <stdint.h>
#include <freeradius-devel/util/heap.h>

typedef struct fr_quickheap_s fr_quickheap_t;

/*
 * Note that there's not a _field parameter in the following.
 * It may well appear as we work to provide the functionality of
 * fr_heap_t.
 */

/** Creates a quickheap that can be used with non-talloced elements
 *
 * @param[in] _ctx		Talloc ctx to allocate quickheap in.
 * @param[in] _cmp		Comparator used to compare elements.
 * @param[in] _type		Of elements.
 */
#define fr_quickheap_alloc(_ctx, _cmp, _type) \
	_fr_quickheap_alloc(_ctx, _cmp, NULL)

/** Creates a quickheap that verifies elements are of a specific talloc type
 *
 * @param[in] _ctx		Talloc ctx to allocate quickheap in.
 * @param[in] _cmp		Comparator used to compare elements.
 * @param[in] _talloc_type	of elements.
 * @return
 *	- A new quickheap.
 *	- NULL on error.
 */
#define fr_quickheap_talloc_alloc(_ctx, _cmp, _talloc_type) \
	_fr_quickheap_alloc(_ctx, _cmp, #_talloc_type)

fr_quickheap_t *_fr_quickheap_alloc(TALLOC_CTX *ctx, fr_heap_cmp_t cmp, char const *type);

void 		*fr_quickheap_peek(fr_quickheap_t *qh) CC_HINT(nonnull);

void 		*fr_quickheap_pop(fr_quickheap_t *qh) CC_HINT(nonnull);

void 		fr_quickheap_insert(fr_quickheap_t *qh, void *data) CC_HINT(nonnull);

uint32_t	fr_quickheap_num_elements(fr_quickheap_t *qh) CC_HINT(nonnull);

#ifdef __cplusplus
}
#endif

