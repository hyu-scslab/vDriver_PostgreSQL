/*-------------------------------------------------------------------------
 *
 * vchain.c
 *    Version chain implementation for each tuple
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/vcluster/vchain.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef HYU_LLT
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/vchain.h"
#include "storage/vchain_hash.h"
#include "utils/snapmgr.h"

/*
 * VChainShmemSize
 *
 * Compute the size of shared memory for the vchain.
 * Physical chain is already embedded in the segment index so that
 * vchain hash table is what we only need to care about.
 */
Size
VChainShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, VChainHashShmemSize(
			NVChainExpected + NUM_VCHAIN_PARTITIONS));

	return size;
}

/*
 * VChainInit
 *
 * Initialize vchain_hash in shared memory.
 * Physical chain is already embedded in the segment index.
 */
void
VChainInit(void)
{
	VChainHashInit(NVChainExpected + NUM_VCHAIN_PARTITIONS);
}

/*
 * VChainLookupLocator
 *
 * Find a version locator of a tuple which has the given primary key
 * and the visible version with the given snapshot.
 * Returns true if it found, false if not found.
 */
bool
VChainLookupLocator(PrimaryKey primary_key,
					Snapshot snapshot,
					VLocator **ret_locator)
{
	LWLock			*partition_lock;
	uint32			hashcode;
	dsa_pointer		dsap_chain;
	VLocator		*chain;
	VLocator		*locator;

	/* Get hash code for the primary key */
	hashcode = VChainHashCode(&primary_key);
	partition_lock = VChainMappingPartitionLock(hashcode);

	/* Acquire partition lock for the primary key with shared mode */
	LWLockAcquire(partition_lock, LW_SHARED);
	if (!VChainHashLookup(&primary_key, hashcode, &dsap_chain))
	{
		/* There is no hash entry for the primary key in the vchain hash */
		LWLockRelease(partition_lock);
		return false;
	}
	LWLockRelease(partition_lock);

	chain = (VLocator *)dsa_get_address(dsa_vcluster, dsap_chain);
	if (chain->dsap_prev == chain->dsap)
	{
		/* There is a hash entry, but the version chain is empty */
		return false;
	}

	/*
	 * Now we have the hash entry (dummy node) that indicates the
	 * head/tail of the version chain.
	 * Try to find the visible version from the recent version (tail)
	 */
	locator = (VLocator *)dsa_get_address(dsa_vcluster, chain->dsap_prev);
	while (locator->dsap != chain->dsap)
	{
		if (!XidInMVCCSnapshot(locator->xmin, snapshot))
		{
			/* Found the visible version */
			*ret_locator = locator;
			return true;
		}
		locator = (VLocator *)dsa_get_address(
				dsa_vcluster, locator->dsap_prev);
	}

	return false;
}

/*
 * VChainAppendLocator
 *
 * Append a vlocator into the version chain of the corresponding tuple.
 */
void
VChainAppendLocator(PrimaryKey primary_key, VLocator *locator)
{
	LWLock			*partition_lock;
	uint32			hashcode;
	dsa_pointer		dsap_chain;
	VLocator		*chain;
	VLocator		*pred;

	/* Get hash code for the primary key */
	hashcode = VChainHashCode(&primary_key);
	partition_lock = VChainMappingPartitionLock(hashcode);

	/* Acquire partition lock for the primary key with shared mode */
	LWLockAcquire(partition_lock, LW_SHARED);
	if (VChainHashLookup(&primary_key, hashcode, &dsap_chain))
	{
		/* Hash entry is already exists */
		LWLockRelease(partition_lock);
	}
	else
	{
		/* Re-acquire the partition lock with exclusive mode */
		LWLockRelease(partition_lock);
		LWLockAcquire(partition_lock, LW_EXCLUSIVE);

		/* Insert a new hash entry for the primary key */
		VChainHashInsert(&primary_key, hashcode, &dsap_chain);
		LWLockRelease(partition_lock);
	}
	
	/*
	 * Now we have the hash entry (dummy node) that indicates the
	 * head/tail of the version chain.
	 * Appending a new version node could compete with the cleaner.
	 * TODO: follow the consensus protocol between transaction/cleaner.
	 */
	chain = (VLocator *)dsa_get_address(dsa_vcluster, dsap_chain);

	/* At this time, just naively implement appending a locator */
	locator->dsap_prev = chain->dsap_prev;
	locator->dsap_next = chain->dsap;
	chain->dsap_prev = locator->dsap;
	if (chain->dsap_next == chain->dsap) /* current chain is empty */
		chain->dsap_next = locator->dsap;
	else
	{
		pred = (VLocator *)dsa_get_address(dsa_vcluster, chain->dsap_next);
		pred->dsap_next = locator->dsap;
	}
}

#endif /* HYU_LLT */
