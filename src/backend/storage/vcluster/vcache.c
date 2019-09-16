/*-------------------------------------------------------------------------
 *
 * vcache.c
 *    Cache implementation for storing version cluster pages in memory
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/vcluster/vcache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/vcache.h"
#include "storage/vcache_hash.h"

VCacheDescPadded	*VCacheDescriptors;
char				*VCacheBlocks;
VCacheMeta		 	*VCache;

#define SEG_PAGESZ					(BLCKSZ)
#define SEG_OFFSET_TO_PAGE_ID(off)  ((off) / (SEG_PAGESZ))

/*
 * Number of reserved pages should be less than the number of pages
 * in a segment.
 */
#define NUM_PAGES_IN_SEG	((VCLUSTER_SEGSIZE) / (SEG_PAGESZ))
#define PAGE_RESERVE		((NUM_PAGES_IN_SEG) / 32)

/* decls for local routines only used within this module */
static int VCacheGetCacheRef(VSegmentId seg_id,
							 VSegmentOffset seg_offset,
							 bool is_append);

static void VCacheReadSegmentPage(const VCacheTag *tag, int cache_id);
static void VCacheWriteSegmentPage(const VCacheTag *tag, int cache_id);
static void VCacheUnref(VCacheDesc *cache);

/*
 * VCacheShmemSize
 *
 * Compute the size of shared memory for the vcache including
 * vcluster pages, vcache descriptors, hash tables, etc.
 */
Size
VCacheShmemSize(void)
{
	Size		size = 0;

	/* size of vcache descriptors */
	size = add_size(size, mul_size(NVCache, sizeof(VCacheDescPadded)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of data pages */
	size = add_size(size, mul_size(NVCache, SEG_PAGESZ));

	/* size of vcache_hash */
	size = add_size(size, VCacheHashShmemSize(NVCache + NUM_VCACHE_PARTITIONS));

	/* size of vcache metadata */
	size = add_size(size, sizeof(VCache));

	return size;
}

/*
 * VCacheInit
 *
 * Initialize vcache, vcache_desc, vcache_hash in shared memory
 *
 */
void
VCacheInit(void)
{
	bool		foundBufs,
				foundDescs,
				foundMeta;
	VCacheDesc *cache;

	/* Align descriptors to a cacheline boundary. */
	VCacheDescriptors = (VCacheDescPadded *)
			ShmemInitStruct("VCache Descriptors",
							NVCache * sizeof(VCacheDescPadded),
							&foundDescs);

	/*
	 * TODO: need to confirm if the cache blocks are aligned properly
	 * for using direct io
	 */
	VCacheBlocks = (char *)
			ShmemInitStruct("VCache Blocks",
							NVCache * (Size) SEG_PAGESZ, &foundBufs);

	/* Initialize the shared vcache lookup hashtable */
	VCacheHashInit(NVCache + NUM_VCACHE_PARTITIONS);

	/* Initialize vcache descriptors */
	for (int i = 0; i < NVCache; i++) 
	{
		cache = GetVCacheDescriptor(i);
		cache->tag.seg_id = cache->tag.page_id = 0;
		cache->is_dirty = false;
		pg_atomic_init_u32(&cache->refcount, 0);
	}

	/* Initialize vcache metadata */
	VCache = (VCacheMeta *)
			ShmemInitStruct("VCache Metadata",
							sizeof(VCacheMeta),
							&foundMeta);
}

/*
 * VCacheAppendTuple
 *
 * Append the given tuple into the cache entry which is containing the
 * corresponding segment page.
 */
void
VCacheAppendTuple(VSegmentId seg_id,
				  VSegmentId reserved_seg_id,
				  VSegmentOffset seg_offset,
				  Size tuple_size,
				  const void *tuple)
{
	int				cache_id;		/* vcache index of target segment page */
	VCacheDesc	   *cache;
	int				page_offset;
	int				written;
	VSegmentOffset	reserved_seg_offset;

	/*
	 * Alined size with power of 2. This is needed because
	 * the current version only support fixed size tuple with power of 2
	 */
	Size			aligned_tuple_size;

	aligned_tuple_size = 1 << my_log2(tuple_size);

	cache_id = VCacheGetCacheRef(seg_id, seg_offset, true);
	cache = GetVCacheDescriptor(cache_id);
	
	page_offset = seg_offset % SEG_PAGESZ;

	/* Copy the tuple into the cache */
	memcpy(&VCacheBlocks[cache_id * SEG_PAGESZ + page_offset],
			tuple, tuple_size);

	/*
	 * Increase written bytes of the cached page
	 * (use aligned size with power of 2)
	 */
	written = pg_atomic_fetch_add_u32(
			&cache->written_bytes, aligned_tuple_size) + aligned_tuple_size;
	Assert(written <= SEG_PAGESZ);

	if (written == SEG_PAGESZ)
	{
		/*
		 * This page has been full, so we should unpin this page.
		 * Mark it as dirty so that it could be flushed when evicted.
		 */
		cache->is_dirty = true;
		VCacheUnref(cache);
	}

	/* Whether or not the page has been full, we should unref the page */
	VCacheUnref(cache);

	/*
	 * OPTIMIZATION: To relax the contention between appending transactions
	 * on exclusive partition hash lock, we proactively reserve and pin
	 * some segment pages that are going to be used soon.
	 */
	if (written == SEG_PAGESZ)
	{
		reserved_seg_offset = seg_offset + PAGE_RESERVE * SEG_PAGESZ;
		if (reserved_seg_offset < VCLUSTER_SEGSIZE)
		{
			/* Reserve a cache entry for the page on same segment */
			cache_id = VCacheGetCacheRef(
					seg_id, reserved_seg_offset, true);
		}
		else
		{
			/* Reserve a cache entry for the page on next segment */
			reserved_seg_offset = seg_offset % VCLUSTER_SEGSIZE;
			cache_id = VCacheGetCacheRef(
					reserved_seg_id, reserved_seg_offset, true);
		}
		cache = GetVCacheDescriptor(cache_id);
		VCacheUnref(cache);
	}
}

/*
 * VCacheReadTuple
 *
 * Read a tuple from the given seg_id and seg_offset.
 * Find the corresponding cache entry, and if there isn't, read the page first.
 */
void
VCacheReadTuple(VSegmentId seg_id,
				VSegmentOffset seg_offset,
				Size tuple_size,
				void *ret_tuple)
{
	int			cache_id;			/* vcache index of target segment page */
	VCacheDesc *cache;
	int			page_offset;

	cache_id = VCacheGetCacheRef(seg_id, seg_offset, false);
	cache = GetVCacheDescriptor(cache_id);

	page_offset = seg_offset % SEG_PAGESZ;

	/* Copy the tuple from the cache */
	memcpy(ret_tuple,
			&VCacheBlocks[cache_id * SEG_PAGESZ + page_offset],
			tuple_size);

	/* We should unref this page */
	VCacheUnref(cache);
}

/*
 * VCacheGetCacheRef
 *
 * Increase refcount of the requested segment page, and returns the cache_id.
 * If the page is not cached, read it from the segment file. If cache is full,
 * evict one page following the eviction policy (currently round-robin..)
 * Caller must decrease refcount after using it. If caller makes the page full
 * by appending more tuple, it has to decrease one more count for unpinning it.
 */
static int
VCacheGetCacheRef(VSegmentId seg_id,
			  	  VSegmentOffset seg_offset,
				  bool is_append)
{
	VCacheTag	vcache_tag;			/* identity of requested block */
	int			cache_id;			/* vcache index of target segment page */
	int			candidate_id;		/* vcache index of victim segment page */
	LWLock	   *new_partition_lock;	/* vcache partition lock for it */
	LWLock	   *old_partition_lock;	/* vcache partition lock for it */
	uint32		hashcode;
	uint32		hashcode_vict;
	VCacheDesc *cache;
	int			ret;
	VCacheTag	victim_tag;

	vcache_tag.seg_id = seg_id;
	vcache_tag.page_id = SEG_OFFSET_TO_PAGE_ID(seg_offset);
	
	/* Get hash code for the segment id & page id */
	hashcode = VCacheHashCode(&vcache_tag);
	new_partition_lock = VCacheMappingPartitionLock(hashcode);

	LWLockAcquire(new_partition_lock, LW_SHARED);
	cache_id = VCacheHashLookup(&vcache_tag, hashcode);
	if (cache_id >= 0)
	{
		/* Target page is already in cache */
		cache = GetVCacheDescriptor(cache_id);

		/* Increase refcount by 1, so this page shoudn't be evicted */
		pg_atomic_fetch_add_u32(&cache->refcount, 1);
		LWLockRelease(new_partition_lock);

		return cache_id;
	}

	/*
	 * Need to acquire exclusive lock for inserting a new vcache_hash entry
	 */
	LWLockRelease(new_partition_lock);
	LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);

	/*
	 * If another transaction already inserted the vcache hash entry,
	 * just use it
	 */
	cache_id = VCacheHashLookup(&vcache_tag, hashcode);
	if (cache_id >= 0)
	{
		cache = GetVCacheDescriptor(cache_id);

		pg_atomic_fetch_add_u32(&cache->refcount, 1);
		LWLockRelease(new_partition_lock);

		return cache_id;
	}

find_cand:
	/* Pick up a candidate cache entry for a new allocation */
	candidate_id = pg_atomic_fetch_add_u64(
			&VCache->eviction_rr_idx, 1) % NVCache;
	cache = GetVCacheDescriptor(candidate_id);
	if (pg_atomic_read_u32(&cache->refcount) != 0)
	{
		/* Someone is accessing this entry, find another candidate */
		goto find_cand;
	}
	victim_tag = cache->tag;

	/*
	 * It seems that this entry is unused now. But we need to check it
	 * again after holding the partition lock, because another transaction
	 * might trying to access and increase this refcount just right now.
	 */
	if (victim_tag.seg_id > 0)
	{
		/*
		 * This entry is using now so that we need to remove vcache_hash
		 * entry for it. We also need to flush it if the cache entry is dirty.
		 */
		hashcode_vict = VCacheHashCode(&cache->tag);
		old_partition_lock = VCacheMappingPartitionLock(hashcode_vict);
		if (LWLockHeldByMe(old_partition_lock))
		{
			/* Partition lock collision occured by myself */
			/*
			 * TODO: Actually, the transaction can use this entry as a victim
			 * by marking lock collision instead of acquiring nested lock.
			 * It will perform better, but now I just simply find another.
			 */
			goto find_cand;
		}

		if (!LWLockConditionalAcquire(old_partition_lock, LW_EXCLUSIVE))
		{
			/* Partition lock is already held by someone. */
			goto find_cand;
		}

		/* Try to hold refcount for the eviction */
		ret = pg_atomic_fetch_add_u32(&cache->refcount, 1);
		if (ret > 0)
		{
			/*
			 * Race occured. Another read transaction might get this page,
			 * or possibly another evicting tranasaction might get this page
			 * if round robin cycle is too short.
			 */
			pg_atomic_fetch_sub_u32(&cache->refcount, 1);
			LWLockRelease(old_partition_lock);
			goto find_cand;
		}

		if (cache->tag.seg_id != victim_tag.seg_id ||
			cache->tag.page_id != victim_tag.page_id)
		{
			/*
			 * This exception might very rare, but the possible scenario is,
			 * 1. txn A processed up to just before holding the
			 *    old_partition_lock
			 * 2. round robin cycle is too short, so txn B acquired the
			 *    old_partition_lock, and evicted this page, and mapped it
			 *    to another vcache_hash entry
			 * 3. Txn B unreffed this page after using it so that refcount
			 *    becomes 0, but seg_id and(or) page_id of this entry have
			 *    changed
			 * In this case, just find another victim for simplicity now.
			 */
			LWLockRelease(old_partition_lock);
			goto find_cand;
		}

		/*
		 * Now we can safely evict this entry.
		 * Remove corresponding hash entry for it so that we can release
		 * the partition lock.
		 */
		VCacheHashDelete(&cache->tag, hashcode_vict);
		LWLockRelease(old_partition_lock);
	}
	else
	{
		/*
		 * This cache entry is unused. Just increase the refcount and use it.
		 */
		ret = pg_atomic_fetch_add_u32(&cache->refcount, 1);
		if (ret > 0)
		{
			/*
			 * Race occured. Possibly another evicting tranasaction might get
			 * this page if round robin cycle is too short.
			 */
			pg_atomic_fetch_sub_u32(&cache->refcount, 1);
			goto find_cand;
		}
	}

	/*
	 * Now we are ready to use this entry as a new cache.
	 * First, check whether this victim should be flushed to segment file.
	 * Appending page shouldn't be picked as a victim because of the refcount.
	 */
	if (cache->is_dirty)
	{
		VCacheWriteSegmentPage(&cache->tag, candidate_id);

		/*
		 * We do not zero the page so that the page could be overwritten
		 * with a new tuple as a new segment page.
		 */
		cache->is_dirty = false;
	}

	/* Initialize the descriptor for a new cache */
	cache->tag = vcache_tag;
	if (!is_append)
	{
		/* Read target segment page into the cache */
		VCacheReadSegmentPage(&cache->tag, candidate_id);
		pg_atomic_write_u32(&cache->written_bytes, SEG_PAGESZ);
	}
	else
	{
		/*
		 * This page is going to be used for appending tuples,
		 * so we pin this page by increasing one more refcount.
		 * Last appender has responsibility for unpinning it.
		 */
		pg_atomic_write_u32(&cache->written_bytes, 0);
		pg_atomic_fetch_add_u32(&cache->refcount, 1);
	}
	
	/* Next, insert new vcache hash entry for it */
	ret = VCacheHashInsert(&vcache_tag, hashcode, candidate_id);
	Assert(ret == -1);
	
	LWLockRelease(new_partition_lock);
	
	/* Return the index of cache entry, holding refcount 1 */
	return candidate_id;
}

/*
 * VCacheUnref
 *
 * Decrease the refcount of the given cache entry
 */
static void
VCacheUnref(VCacheDesc *cache)
{
	pg_atomic_fetch_sub_u32(&cache->refcount, 1);
}

/*
 * VCacheReadSegmentPage
 *
 * Read target segment page into the cache block.
 * If the segment file has not opened yet, open it and store the fd.
 */
static void
VCacheReadSegmentPage(const VCacheTag *tag, int cache_id)
{
	/*
	 * NOTE: Assumes that all segment files are already opened so that
	 * we don't have to open the file here.
	 */
	int ret;

	ret = pread(seg_fds[tag->seg_id],
				&VCacheBlocks[cache_id * SEG_PAGESZ],
				SEG_PAGESZ, tag->page_id * SEG_PAGESZ);
	
	Assert(ret == SEG_PAGESZ);
}

/*
 * VCacheWriteSegmentPage
 *
 * Flush a cache entry into the target segment file.
 */
static void
VCacheWriteSegmentPage(const VCacheTag *tag, int cache_id)
{
	/*
	 * NOTE: Assumes that all segment files are already opened so that
	 * we don't have to open the file here.
	 */
	int ret;

	ret = pwrite(seg_fds[tag->seg_id],
				 &VCacheBlocks[cache_id * SEG_PAGESZ],
				 SEG_PAGESZ, tag->page_id * SEG_PAGESZ);

	Assert(ret == SEG_PAGESZ);
}

