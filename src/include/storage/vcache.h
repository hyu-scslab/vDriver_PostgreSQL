/*-------------------------------------------------------------------------
 *
 * vcache.h
 *	  hash table definitions for mapping VCacheTag to vcache indexes.
 *
 *
 *
 * src/include/storage/vcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VCACHE_H
#define VCACHE_H

#include "port/atomics.h"
#include "storage/vcluster.h"

typedef struct VCacheTag
{
	VSegId		vseg_id;
	VSegPageId	vseg_offset;
} VCacheTag;

typedef struct VCacheDesc
{
	VCacheTag	tag;			/* ID of page contained in cahce */
	int			cache_id;		/* cache's index number (from 0) */

	bool		pinned;			/* pinned page cannot be evicted */
	bool		is_dirty;		/* whether the page is not yet synced */
	
	/* page with refcount > 0 cannot be evicted */
	pg_atomic_uint32	refcount;
} VCacheDesc;

#define VCACHEDESC_PAD_TO_SIZE	(SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union VCacheDescPadded
{
	VCacheDesc	vcachedesc;
	char		pad[VCACHEDESC_PAD_TO_SIZE];
} VCacheDescPadded;

/* in globals.c ... this duplicates miscadmin.h */
extern PGDLLIMPORT int NVCache;

extern Size VCacheShmemSize(void);
extern void VCacheInit(void);

#endif							/* VCACHE_H */
