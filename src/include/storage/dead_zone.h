/*-------------------------------------------------------------------------
 *
 * dead_zone.h
 *	  dead zone
 *
 *
 *
 * src/include/storage/dead_zone.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DEAD_ZONE_H
#define DEAD_ZONE_H

#include "c.h"
#include "utils/dsa.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"




/* dead zone descriptor */
typedef struct {
	TransactionId		dead_zone;
} DeadZoneDesc;




extern DeadZoneDesc*	dead_zone_desc;

extern Size DeadZoneShmemSize(void);
extern void DeadZoneInit(void);
extern pid_t StartDeadZoneUpdater(void);

extern void SetDeadZone(void);
extern TransactionId GetDeadZone(void);
extern bool IsInDeadZone(TransactionId xmin,
						 TransactionId xmax);


#endif							/* DEAD_ZONE_H */
