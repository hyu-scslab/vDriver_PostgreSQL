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

#include "c.h"
#include "utils/dsa.h"

typedef enum {
	VCLUSTER_HOT,
	VCLUSTER_COLD,
	VCLUSTER_LLT,
	VCLUSTER_NUM = 3
} VCLUSTER_TYPE;

/* Size of a segment of a version cluster */
#define VCLUSTER_SEGSIZE    (16*1024*1024)

/* Version tuple size */
#define VCLUSTER_TUPLE_SIZE	(128)	/* TODO: move this to configuration */

/* Number of tuple entry in a vsegment */
#define VCLUSTER_SEG_NUM_ENTRY	((VCLUSTER_SEGSIZE) / (VCLUSTER_TUPLE_SIZE))

typedef uint32_t VSegmentId;
typedef uint32_t VSegmentOffset;
typedef uint32_t VSegmentPageId;

typedef struct {
	TransactionId		xmin;
	VSegmentId			seg_id;
	VSegmentOffset		seg_offset;

	/* Version chain pointer for a single tuple */
	struct VLocator		*prev;
	struct VLocator		*next;
} VLocator;

typedef struct {
	VSegmentId			seg_id;
	TransactionId		xmin;
	TransactionId		xmax;
	
	/* Need to be converted from dsa_pointer to (VSegmentDesc *) */
	dsa_pointer			*next;

	/* Segment offset where the next version tuple will be appended.
	 * Backend process will use fetch-and-add instruction on this variable
	 * to allocate the offset, so the value can be higher than the segment
	 * size. In this case, one of the backend process who get this high value
	 * should open a new segment. */
	pg_atomic_uint32	seg_offset;

	/* Just embed segment index into here  */
	VLocator			locators[VCLUSTER_SEG_NUM_ENTRY];
} VSegmentDesc;

typedef struct {
	/* need to be converted from dsa_pointer to (VSegmentDesc *) */
	dsa_pointer			head[VCLUSTER_NUM];

	/* next segment id for allocation */
	pg_atomic_uint32	next_seg_id;

	/* reserved segment id for each cluster */
	VSegmentId			reserved_seg_id[VCLUSTER_NUM];
} VClusterDesc;

extern VClusterDesc	*vclusters;
extern dsa_area		*dsa_vcluster;

#define VCLUSTER_MAX_SEGMENTS		10000
extern int seg_fds[VCLUSTER_MAX_SEGMENTS];

extern Size VClusterShmemSize(void);
extern void VClusterShmemInit(void);
extern void VClusterDsaInit(void);
extern void VClusterAttachDsa(void);
extern void VClusterDetachDsa(void);

extern void VClusterAppendTuple(VCLUSTER_TYPE cluster_type,
								Size tuple_size,
								void *tuple);

#endif							/* VCLUSTER_H */
