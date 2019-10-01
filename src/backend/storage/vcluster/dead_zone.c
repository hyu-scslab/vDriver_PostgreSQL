/*-------------------------------------------------------------------------
 *
 * dead_zone.c
 *
 * Dead Zone Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/vcluster/dead_zone.c
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
#include "storage/procarray.h"
#include "storage/lwlock.h"
#include "postmaster/fork_process.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "tcop/tcopprot.h"
#include "storage/sinvaladt.h"
#include "libpq/pqsignal.h"

#include "storage/vcluster.h"

#include "storage/dead_zone.h"



/* dead zone in shared memory*/
DeadZoneDesc*	dead_zone_desc;

/* local functions */
static void DeadZoneUpdaterProcessMain(void);




/*
 * DeadZoneShmemSize
 *
 * compute the size of shared memory for the dead zone
 */
Size
DeadZoneShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(DeadZoneDesc));

	return size;
}

/*
 * DeadZoneInit
 *
 * Initialize shared data structures related to dead zone in shared memory
 */
void
DeadZoneInit(void)
{
	bool		foundDesc;

	//assert(PROCARRAY_MAXPROCS < THREAD_TABLE_SIZE);

	dead_zone_desc = (DeadZoneDesc*)
		ShmemInitStruct("Dead Zone Descriptor",
						sizeof(DeadZoneDesc), &foundDesc);

	dead_zone_desc->dead_zone = 0;
}

/*
 * StartDeadZoneUpdater
 *
 * Fork dead zone updater process.
 */
pid_t
StartDeadZoneUpdater(void)
{
	pid_t		updater_pid;

	/* Start a cutter process. */
	updater_pid = fork_process();
	if (updater_pid == -1) {
		/* error */
	}
	else if (updater_pid == 0) {
		/* child */
		DeadZoneUpdaterProcessMain(); /* no return */
	}
	else {
		/* parent */
	}

	return updater_pid;
}

/*
 * SetDeadZone
 *
 * TODO: need to represent dead zone more specific.
 * TODO: it is very naive implement!!
 */
void
SetDeadZone(void)
{
	/* TODO: Now, we just set dead zone by oldest active trx id. */

	dead_zone_desc->dead_zone = GetOldestActiveTransactionId() - 50000;
}


/*
 * GetDeadZone
 *
 * TODO: need to represent dead zone more specific.
 * TODO: it is very naive implement!!
 */
TransactionId
GetDeadZone(void)
{
	return dead_zone_desc->dead_zone;
}


/*
 * IsInDeadZone
 *
 * Decide whether this version(record, segment desc) is dead or not.
 * We only judge by dead zone, xmin and xmax.
 * TODO: it is very naive implement!!
 */
bool
IsInDeadZone(TransactionId xmin, TransactionId xmax)
{
	bool ret = false;

	assert(xmin <= xmax);

	if (xmax < GetDeadZone())
		ret = true;
	else
		ret = false;

	return ret;
}

/*
 * DeadZoneUpdaterProcessMain
 *
 * Main function of dead zone updater process.
 */
static void
DeadZoneUpdaterProcessMain(void)
{
	/* just copy routine to hear from other process-generation-code */
	/* ex) StartAutoVacLauncher */
	InitPostmasterChild();
	ClosePostmasterPorts(false);
	SetProcessingMode(InitProcessing);
	pqsignal(SIGQUIT, my_quick_die);
	pqsignal(SIGTERM, my_quick_die);
	pqsignal(SIGKILL, my_quick_die);
	pqsignal(SIGINT, my_quick_die);
	PG_SETMASK(&UnBlockSig);
	BaseInit();
	InitProcess();

	for (;;) {
		elog(WARNING, "HYU_LLT : updater looping..");
		SetDeadZone();
		sleep(1);
	}
}


#endif /* HYU_LLT */
