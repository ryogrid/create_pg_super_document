# StartupXLOG

## Location
[src/backend/access/transam/xlog.c:5384-6187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L5384-L6187)

## Overview
StartupXLOG is the main recovery function that must be called ONCE during postmaster or standalone-backend startup to perform WAL (Write-Ahead Log) recovery and bring the database system to a consistent state.

## Definition

```c
struct a RunningTransactions snapshot representing a
				 * shut down server, with only prepared transactions still
				 * alive. We're never overflowed at this point because all
				 * subxids are listed with their parent prepared transactions.
				 */
				running.xcnt = nxids;
```
## Detailed Description
StartupXLOG is a comprehensive function responsible for orchestrating the entire database recovery process during startup. It handles multiple recovery scenarios including clean shutdowns, crash recovery, and archive recovery (point-in-time recovery). The function performs the following major operations:

1. **Control File Validation**: Examines the control file state to determine the database's previous shutdown condition and validates checkpoint locations
2. **Directory Structure Setup**: Ensures WAL directory structure exists and removes temporary files from previous crashes
3. **Recovery Initialization**: Sets up recovery state, initializes shared memory structures, and prepares for WAL replay
4. **WAL Recovery**: Performs actual WAL record replay if needed, restoring the database to a consistent state
5. **Timeline Management**: Handles timeline switching for archive recovery scenarios
6. **System Transition**: Transitions the database from recovery mode to production mode

The function handles various database states (DB_SHUTDOWNED, DB_IN_CRASH_RECOVERY, DB_IN_ARCHIVE_RECOVERY, etc.) and performs appropriate recovery actions for each scenario. It also manages Hot Standby initialization, prepared transaction recovery, and ensures proper synchronization of shared memory structures.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [ValidateXLOGDirectoryStructure](../V/ValidateXLOGDirectoryStructure.md)
  - [InitWalRecovery](../I/InitWalRecovery.md)
  - [PerformWalRecovery](../P/PerformWalRecovery.md)
  - [FinishWalRecovery](../F/FinishWalRecovery.md)
  - [PrescanPreparedTransactions](../P/PrescanPreparedTransactions.md)
  - [RecoverPreparedTransactions](../R/RecoverPreparedTransactions.md)
  - [findNewestTimeLine](../f/findNewestTimeLine.md)
  - [writeTimeLineHistory](../w/writeTimeLineHistory.md)
  - [XLogInitNewTimeline](../X/XLogInitNewTimeline.md)
  - [StartupCLOG](StartupCLOG.md), StartupMultiXact, StartupReplicationSlots
  - [RemoveTempXlogFiles](../R/RemoveTempXlogFiles.md)
  - ResetUnloggedRelations
  - [PerformRecoveryXLogAction](../P/PerformRecoveryXLogAction.md)
  - [PreallocXlogFiles](../P/PreallocXlogFiles.md)
- Called from (representative examples):
  - [StartupProcessMain](StartupProcessMain.md) (startup process entry point)
  - [InitPostgres](../I/InitPostgres.md) (single-user mode startup)

## Notes and Other Information
- Must be called exactly once during database startup
- Sets up resource owner context for auxiliary processes
- Handles both crash recovery (extending current timeline) and archive recovery (creating new timeline)  
- Manages transition from InRecovery=true to InRecovery=false state
- Critical for database consistency - ensures all committed transactions are replayed
- Coordinates with Hot Standby functionality when enabled
- Handles cleanup of backup label and tablespace map files after successful recovery
- Updates control file state to DB_IN_PRODUCTION upon completion
- Located in src/backend/access/transam/xlog.c:5384-6187