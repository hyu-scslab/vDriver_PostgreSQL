/*-------------------------------------------------------------------------
 *
 * vchain_hash.c
 *
 * hash table implementation for managing head/tail of version chain for
 * each tuple using its primary key
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/vcluster/vchain_hash.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/vchain_hash.h"
#include "utils/hsearch.h"

/* entry for version chain lookup hashtable */
typedef struct
{
	/* Primary key, assume we only use 64 bit integer at this time */
	PrimaryKey	key;

	/*
	 * head/tail of the chain. prev field indicates the tail node, and
	 * next field indicates the head node.
	 */
	VLocator	vlocator;
} VChainLookupEnt;

static HTAB *SharedVChainHash;

/*
 * VChainHashShmemSize
 *
 * compute the size of shared memory for vchain_hash
 */
Size
VChainHashShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(VChainLookupEnt));
}

/*
 * VChainHashInit
 *
 * Initialize vchain_hash in shared memory
 */
void
VChainHashInit(int size)
{
	HASHCTL		info;

	/* Primary key maps to version chain */
	info.keysize = sizeof(PrimaryKey);
	info.entrysize = sizeof(VChainLookupEnt);
	info.num_partitions = NUM_VCHAIN_PARTITIONS;

	SharedVChainHash = ShmemInitHash("Shared VChain Lookup Table",
								     size, size,
								     &info,
								     HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

/*
 * VChainHashCode
 *
 * Compute the hash code associated with a primary key
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
VChainHashCode(const PrimaryKey *tagPtr)
{
	return get_hash_value(SharedVChainHash, (void *) tagPtr);
}

/*
 * VChainHashLookup
 *
 * Lookup the given primary key; set ret to vlocator(head/tail) of the vchain
 * and returns true if found. set ret to NULL and returns false if not found.
 * Caller must hold at least share lock on VChainMappingLock for tag's partition
 */
bool
VChainHashLookup(const PrimaryKey *tagPtr, uint32 hashcode, VLocator *ret)
{
	VChainLookupEnt *result;

	result = (VChainLookupEnt *)
			hash_search_with_hash_value(SharedVChainHash,
										(void *) tagPtr,
										hashcode,
										HASH_FIND,
										NULL);
	if (!result)
	{
		ret = NULL;
		return false;
	}

	ret = &result->vlocator;
	return true;
}

/*
 * VChainHashInsert
 *
 * Insert a hashtable entry for given primary key and head/tail of the chain,
 * unless an entry already exists for that key.
 * New hash table entry (VLocator) will always be intialized so that the
 * caller should insert the first version of the chain itself.
 *
 * Returns true and set ret to new entry on successful insertion.
 * If a conflicting entry exists already, set ret to existing entry and
 * returns false.
 *
 * Caller must hold exclusive lock on VChainMappingLock for tag's partition
 */
bool
VChainHashInsert(const PrimaryKey *tagPtr, uint32 hashcode, VLocator *ret)
{
	VChainLookupEnt    *result;
	bool				found;
	VLocator			vlocator;

	memset(&vlocator, 0, sizeof(VLocator));
	
	result = (VChainLookupEnt *)
			hash_search_with_hash_value(SharedVChainHash,
										(void *) tagPtr,
										hashcode,
										HASH_ENTER,
										&found);

	if (found)
	{
		/* found something already in the table */
		ret = &result->vlocator;
		return false;
	}

	result->vlocator = vlocator;
	ret = &result->vlocator;

	return true;
}

/*
 * VChainHashDelete
 *
 * Delete the hashtable entry for given primary key (which must exist)
 *
 * Caller must hold exclusive lock on VChainMappingLock for pkey's partition
 */
void
VChainHashDelete(const PrimaryKey *tagPtr, uint32 hashcode)
{
	VChainLookupEnt *result;

	result = (VChainLookupEnt *)
			hash_search_with_hash_value(SharedVChainHash,
										(void *) tagPtr,
										hashcode,
										HASH_REMOVE,
										NULL);
	
	if (!result)				/* shouldn't happen */
		elog(ERROR, "shared vchain hash table corrupted");
}

/* TODO: make a corresponding header file, and need to think about removing vlocator parameter of insertion api */
