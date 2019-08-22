/*-------------------------------------------------------------------------
 *
 * vcache_hash.h
 *	  hash table definitions for mapping VCacheTag to vcache indexes.
 *
 *
 *
 * src/include/storage/vcache_hash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VCACHE_HASH_H
#define VCACHE_HASH_H

extern Size VCacheHashShmemSize(int size);
extern void VCacheHashInit(int size);

#endif							/* VCACHE_HASH_H */
