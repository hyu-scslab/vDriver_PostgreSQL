/*-------------------------------------------------------------------------
 *
 * vstatistic.h
 *	  statistics about vDriver
 *
 *
 *
 * src/include/storage/vstatistic.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VSTATISTIC_H
#define VSTATISTIC_H

#include "c.h"
#include "utils/dsa.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#define NUM_CUTTIME_BUCKET		1000
#define CUTTIME_BUCKET_UNIT		1000	/* x microsec */


/* vDriver statistics descriptor */
typedef struct {
	/* number of inserted-records */
	int64_t			cnt_inserted;

	/* about pruned-records */
	int64_t			cnt_first_prune;

	/* number of records which is not first-pruned */
	int64_t			cnt_after_first_prune;

	/* number of evicted-pages */
	int64_t			cnt_page_evicted;

	/* about second-pruned-pages */
	int64_t			cnt_page_second_prune;

	/* number of logically-deleted-records */
	int64_t			cnt_logical_deleted;

	/* number of logically-deleted-segment */
	int64_t			cnt_seg_logical_deleted;

	/* number of physically-deleted-segment */
	int64_t			cnt_seg_physical_deleted;

	/* bucket of segment cuttime */
	uint32			bucket_cuttime[NUM_CUTTIME_BUCKET];
} VStatisticDesc;




extern VStatisticDesc*	vstatistic_desc;

extern Size VStatisticShmemSize(void);
extern void VStatisticInit(void);
extern void VStatisticUpdateCuttime(uint64 cuttime_us);

extern pid_t StartMonitor(void);


#endif							/* VSTATISTIC_H */
