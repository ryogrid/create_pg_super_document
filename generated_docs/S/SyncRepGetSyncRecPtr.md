# SyncRepGetSyncRecPtr

## Location
[src/backend/replication/syncrep.c:586-659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L586-L659)

## Overview
Calculates the synchronized Write, Flush, and Apply positions among synchronous standbys and determines if the current WAL sender is managing a sync standby.

## Definition

```c
static bool
SyncRepGetSyncRecPtr(XLogRecPtr *writePtr, XLogRecPtr *flushPtr,
					 XLogRecPtr *applyPtr, bool *am_sync)
```
## Detailed Description
This internal function is the core logic for determining synchronous replication positions in PostgreSQL. It examines all candidate synchronous standbys and calculates the appropriate LSN positions that can be considered "synchronized" based on the configured synchronous replication method.

The function supports two synchronous replication methods:
- **Priority-based**: Uses the oldest (most conservative) positions among sync standbys via 
- **Quorum-based**: Uses the Nth latest positions among sync standbys via 

The function performs validation to ensure sufficient synchronous standbys are available and determines whether the current WAL sender is among the synchronous standbys.

## Parameters / Member Variables
- : Output parameter - receives the synchronized write LSN position
- : Output parameter - receives the synchronized flush LSN position  
- : Output parameter - receives the synchronized apply LSN position
- : Output parameter - set to true if current WAL sender is a sync standby

## Dependencies
- Functions called/Symbols referenced:
  -  - Gets list of candidate synchronous standbys
  -  - Calculates oldest positions (priority method)
  -  - Calculates Nth latest positions (quorum method)
  -  - Data structure for standby information
  -  - Constant for priority-based sync replication
- Called from:
  -  (src/backend/replication/syncrep.c:104)
  -  (src/backend/replication/syncrep.c:516)

## Notes and Other Information
- Returns false if synchronous replication is not configured or insufficient sync standbys are available
- Returns true and populates output parameters when synchronization positions can be determined
- Uses different algorithms based on  (priority vs quorum)
- The function notes that  is more efficient than  for calculating oldest positions
- Memory allocated by  is properly freed with 
- Function location: src/backend/replication/syncrep.c:586-659

## Simplified Source

```c
static bool
SyncRepGetSyncRecPtr(XLogRecPtr *writePtr, XLogRecPtr *flushPtr,
                     XLogRecPtr *applyPtr, bool *am_sync)
{
    SyncRepStandbyData *sync_standbys;
    int num_standbys;

    // Initialize default results
    *writePtr = InvalidXLogRecPtr;
    *flushPtr = InvalidXLogRecPtr;
    *applyPtr = InvalidXLogRecPtr;
    *am_sync = false;

    // Quick exit if synchronous replication not configured
    if (SyncRepConfig == NULL) {
        return false;
    }

    // Get list of candidate synchronous standbys
    num_standbys = SyncRepGetCandidateStandbys(&sync_standbys);

    // Check if current WAL sender is among sync standbys
    for (int i = 0; i < num_standbys; i++) {
        if (sync_standbys[i].is_me) {
            *am_sync = true;
            break;
        }
    }

    // Verify we have enough sync standbys
    if (!(*am_sync) || num_standbys < SyncRepConfig->num_sync) {
        pfree(sync_standbys);
        return false;
    }

    // Calculate sync positions based on replication method
    if (SyncRepConfig->syncrep_method == SYNC_REP_PRIORITY) {
        // Priority-based: use oldest positions
        SyncRepGetOldestSyncRecPtr(writePtr, flushPtr, applyPtr,
                                   sync_standbys, num_standbys);
    } else {
        // Quorum-based: use Nth latest positions
        SyncRepGetNthLatestSyncRecPtr(writePtr, flushPtr, applyPtr,
                                      sync_standbys, num_standbys,
                                      SyncRepConfig->num_sync);
    }

    pfree(sync_standbys);
    return true;
}
```