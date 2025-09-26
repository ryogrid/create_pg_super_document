# SnapBuildProcessRunningXacts

## Location
src/backend/replication/logical/snapbuild.c: 1274 - 1375

## Overview
Processes running transaction records from WAL to build historic snapshots and manages transaction cleanup and replication slot advancement during logical replication.

## Definition


## Detailed Description
SnapBuildProcessRunningXacts is a key function in PostgreSQL's logical replication system that processes xl_running_xacts WAL records to maintain consistent snapshots and manage transaction tracking. The function operates in different modes depending on the current snapshot building state:

**Pre-Consistent State Processing:**
- Calls SnapBuildFindSnapshot to attempt to reach a consistent snapshot state
- Uses the running transaction information to determine if consistency can be achieved
- Returns early if no cleanup is beneficial yet

**Post-Consistent State Processing:**
- Serializes the current snapshot state to disk for persistence and recovery
- Updates the builder's xmin to match the oldest running transaction
- Performs cleanup of transactions no longer needed for replication

**Replication Slot Management:**
- Advances the replication slot's xmin to allow vacuum to clean up protected tuples
- Updates the restart decoding position based on the oldest in-progress transaction
- Ensures efficient storage management by not holding onto unnecessary transaction data

The function balances between maintaining enough historical information for consistent replication while allowing the database to clean up old data that's no longer needed.

## Parameters / Member Variables
- : The SnapBuild context tracking the current snapshot building state
- : Log sequence number of the running xacts record being processed
- : Pointer to the xl_running_xacts record containing transaction state information

## Dependencies
- Functions called/Symbols referenced:
  - SnapBuildFindSnapshot
  - SnapBuildSerialize
  - SnapBuildPurgeOlderTxn
  - ReorderBufferGetOldestXmin
  - ReorderBufferGetOldestTXN
  - LogicalIncreaseXminForSlot
  - LogicalIncreaseRestartDecodingForSlot
- Called from (representative examples):
  - standby_decode (during WAL record processing)

## Notes and Other Information
- Central to the logical replication snapshot building process
- Handles both the initial consistency-building phase and ongoing maintenance
- xmax is intentionally not updated here - only done for catalog transactions in SnapBuildCommitTxn for efficiency
- The function includes sophisticated logic for restart position management to minimize WAL retention
- Uses DEBUG3 logging level for detailed transaction tracking information
- Critical for vacuum advancement - allows cleanup of tuples protected by this replication slot
- Serialization only occurs when in consistent state to avoid unnecessary I/O operations
- The restart decoding position management prevents excessive WAL accumulation during long-running logical replication