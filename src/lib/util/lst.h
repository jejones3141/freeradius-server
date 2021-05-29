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

/** Structures and prototypes for leftmost skeleton trees (LSTs)
 *
 * @file src/lib/util/lst.h
 *
 * @copyright 2021  Network RADIUS SARL (legal@networkradius.com)
 */
RCSIDH(lst_h, "")

#ifdef __cplusplus
extern "C" {
#endif

#include <freeradius-devel/build.h>
#include <freeradius-devel/util/talloc.h>

#include <stdint.h>
#include <freeradius-devel/util/heap.h>

typedef struct fr_lst_s fr_lst_t;

/** Creates an LST that can be used with non-talloced elements
 *
 * @param[in] _ctx		Talloc ctx to allocate LST in.
 * @param[in] _cmp		Comparator used to compare elements.
 * @param[in] _type		Of elements.
 * @param[in] _field		to store heap indexes in.
 */
#define fr_lst_alloc(_ctx, _cmp, _type, _field) \
	_fr_lst_alloc(_ctx, _cmp, NULL, (size_t)offsetof(_type, _field))

/** Creates an LST that verifies elements are of a specific talloc type
 *
 * @param[in] _ctx		Talloc ctx to allocate LST in.
 * @param[in] _cmp		Comparator used to compare elements.
 * @param[in] _talloc_type	of elements.
 * @param[in] _field		to store heap indexes in.
 * @return
 *	- A pointer to the new LST.
 *	- NULL on error.
 */
#define fr_lst_talloc_alloc(_ctx, _cmp, _talloc_type, _field) \
	_fr_lst_alloc(_ctx, _cmp, #_talloc_type, (size_t)offsetof(_type, _field))

fr_lst_t *_fr_lst_alloc(TALLOC_CTX *ctx, fr_heap_cmp_t cmp, char const *type, size_t offset) CC_HINT(nonnull(2));

void 	*fr_lst_peek(fr_lst_t *lst) CC_HINT(nonnull);

void 	*fr_lst_pop(fr_lst_t *lst) CC_HINT(nonnull);

int 	fr_lst_insert(fr_lst_t *lst, void *data) CC_HINT(nonnull);

/** Remove an element from an LST
 *
 * @param[in] lst		the LST to remove an element from
 * @param[in] data		if non-NULL, the element to remove; if NULL, pop and discard the "minimum"
 * @return
 *	- 0 if removal succeeds
 * 	- -1 if removal fails
 */
int	fr_lst_extract(fr_lst_t *lst, void *data) CC_HINT(nonnull(1));

int32_t	fr_lst_num_elements(fr_lst_t *lst) CC_HINT(nonnull);

#ifdef __cplusplus
}
#endif
