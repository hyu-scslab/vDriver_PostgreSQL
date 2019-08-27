/*-------------------------------------------------------------------------
 *
 * vcluster.h
 *	  version cluster
 *
 *
 *
 * src/include/storage/vcluster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VCLUSTER_H
#define VCLUSTER_H

#include "utils/dsa.h"

/* Size of a segment of a version cluster */
#define VCLUSTER_SEGSIZE    (16*1024*1024)

typedef uint32_t VSegId;
typedef uint32_t VSegOffset;
typedef uint32_t VSegPageId;

extern Size VClusterShmemSize(void);
extern void VClusterShmemInit(void);

#endif							/* VCLUSTER_H */
