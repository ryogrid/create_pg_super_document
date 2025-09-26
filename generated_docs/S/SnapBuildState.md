# SnapBuildState

## Location
src/include/replication/snapbuild.h: 47 - 50

## Overview
SnapBuildState is an enumeration that tracks the progression stages of snapshot building machinery used in PostgreSQL's logical replication system to build historic catalog snapshots from WAL records.

## Definition

```c
struct to the public */
struct SnapBuild;
```
## Detailed Description
SnapBuildState represents the state machine for PostgreSQL's snapshot building process used in logical replication. The snapshot building machinery constructs historic catalog snapshots by reading and interpreting the WAL stream, enabling the decoding of heap tuple changes for logical replication purposes.

The state transitions follow a specific sequence that ensures consistent and safe logical decoding:

1. **Initialization Phase (SNAPBUILD_START)**: The machinery starts in this initial state where minimal functionality is available.

2. **Snapshot Building Phase (SNAPBUILD_BUILDING_SNAPSHOT)**: The system begins collecting information about committed transactions to construct the initial catalog snapshot. This involves tracking catalog-modifying transactions.

3. **Full Snapshot Phase (SNAPBUILD_FULL_SNAPSHOT)**: At this stage, sufficient information has been collected to decode tuples in new transactions. However, changes cannot yet be applied because they might depend on transactions that were still running when this state was reached.

4. **Consistent State (SNAPBUILD_CONSISTENT)**: This is the final operational state where all previously running transactions have finished, making it safe to apply commit callbacks and begin actual logical decoding.

The state machine ensures that logical replication starts from a consistent point where all necessary catalog information is available and no partial transaction effects are included in the decoded stream.

## Parameters / Member Variables
- : Initial state with limited functionality
- : State for collecting committed transactions to build catalog snapshot  
- : State where tuple decoding is possible but changes cannot be applied yet
- : Final operational state where consistent decoding and change application is possible

## Dependencies
- Functions called/Symbols referenced:
  - SnapBuild (struct definition in snapbuild.h:50)

- Used by (representative examples):
  - SnapBuild struct (as state field in snapbuild.c:155)
  - SnapBuildCurrentState function (returns this enum in snapbuild.c:412-419)
  - Various state transition logic throughout snapbuild.c

## Notes and Other Information
- The enum values are specifically chosen with SNAPBUILD_START as -1 to clearly distinguish the initial state
- State transitions are unidirectional and follow the sequence: START → BUILDING_SNAPSHOT → FULL_SNAPSHOT → CONSISTENT
- Some transitions can skip intermediate states (e.g., START can go directly to CONSISTENT if no running transactions exist)
- The state machine is critical for ensuring data consistency in logical replication by preventing decoding of incomplete or inconsistent transaction states
- State transitions are triggered by processing xl_running_xacts records and monitoring transaction completion
- The CONSISTENT state is the target state where logical replication can safely begin streaming changes