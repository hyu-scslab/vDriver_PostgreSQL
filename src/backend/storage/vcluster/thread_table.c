/*-------------------------------------------------------------------------
 *
 * thread_table.c
 *
 * Thread Table Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/vcluster/thread_table.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef HYU_LLT
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/vcluster.h"

#include "storage/thread_table.h"

/* thread table in shared memory*/
ThreadTableDesc*	thread_table_desc;

/*
 * ThreadTableShmemSize
 *
 * compute the size of shared memory for the thread table
 */
Size
ThreadTableShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(ThreadTableDesc));

	return size;
}

/*
 * ThreadTableInit
 *
 * Initialize shared data structures related to thread table in shared memory
 */
void
ThreadTableInit(void)
{
	bool		foundDesc;

	//assert(PROCARRAY_MAXPROCS < THREAD_TABLE_SIZE);

	thread_table_desc = (ThreadTableDesc*)
		ShmemInitStruct("Thread Table Descriptor",
						sizeof(ThreadTableDesc), &foundDesc);

	/* Initialize thread table */
	for (int i = 0; i < THREAD_TABLE_SIZE; i++)
	{
		thread_table_desc->thread_table[i].timestamp = TS_NONE;
	}
}


/*
 * SetTimestamp
 *
 * Set caller's timestamp on thread table.
 */
void
SetTimestamp(void)
{
	int index = MyProc->pgprocno;
	ThreadTable thread_table = thread_table_desc->thread_table;

	thread_table[index].timestamp = GetCurrentTimestamp();
}

/*
 * ClearTimestamp
 *
 * Clear caller's timestamp on thread table.
 */
void
ClearTimestamp(void)
{
	int index = MyProc->pgprocno;
	ThreadTable thread_table = thread_table_desc->thread_table;

	thread_table[index].timestamp = TS_NONE;
}

/*
 * GetMinimumTimestamp
 *
 * Return the minimum timestamp from thread table.
 * If timestamp of a object is smaller than GetMinimumTimestamp(), 
 * we can be sure that any other processes can't see the object.
 */
TimestampTz
GetMinimumTimestamp(void)
{
	TimestampTz min_ts;

	min_ts = GetCurrentTimestamp();
	for (int i = 0; i < THREAD_TABLE_SIZE; i++)
	{
		TimestampTz ts;
		ts = thread_table_desc->thread_table[i].timestamp;

		if (ts == TS_NONE)
			continue;

		if (ts < min_ts)
			min_ts = ts;
	}

	return min_ts;
}
#endif /* HYU_LLT */
