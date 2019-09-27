/*-------------------------------------------------------------------------
 *
 * thread_table.h
 *	  thread table
 *
 *
 *
 * src/include/storage/thread_table.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef THREAD_TABLE_H
#define THREAD_TABLE_H

#include "c.h"
#include "utils/dsa.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"


/* Size of thread table : Max number of simultaneous running processes */
/* This value is from procArray->maxProcs(122) */
#define THREAD_TABLE_SIZE	(150)



#define TS_NONE		(0)

/* It may need more variables EX) snapshot */
typedef struct {
	/* FIXME: is need to use volatile?? */
	volatile TimestampTz		timestamp;
} ThreadTableNode;

typedef ThreadTableNode* ThreadTable;



/* thread table descriptor */
typedef struct {
	ThreadTableNode		thread_table[THREAD_TABLE_SIZE];
} ThreadTableDesc;




extern ThreadTableDesc*	thread_table_desc;

extern Size ThreadTableShmemSize(void);
extern void ThreadTableInit(void);
extern void SetTimestamp(void);
extern void ClearTimestamp(void);


#endif							/* THREAD_TABLE_H */
