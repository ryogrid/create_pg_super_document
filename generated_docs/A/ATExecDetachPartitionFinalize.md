# ATExecDetachPartitionFinalize

## Location
[src/backend/commands/tablecmds.c:19646-19680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L19646-L19680)

## Overview
ATExecDetachPartitionFinalize implements the ALTER TABLE DETACH PARTITION FINALIZE command, completing a previously interrupted concurrent partition detachment operation.

## Definition
```c
static ObjectAddress ATExecDetachPartitionFinalize(Relation rel, RangeVar *name)
```

## Detailed Description
This function provides a mechanism to complete partition detachment when a previous DETACH PARTITION CONCURRENTLY operation did not run to completion due to interruption (such as transaction abort or system crash). It serves as a recovery mechanism that ensures the detachment process can be properly finished.

The function implements a critical safety mechanism by waiting for all existing snapshots that might have seen the partition as still attached to complete before proceeding with the finalization. This prevents inconsistent catalog views and ensures that all transactions see a consistent state.

The function delegates the actual finalization work to DetachPartitionFinalize(), passing appropriate parameters to indicate this is a concurrent operation completion.

## Parameters / Member Variables
- `rel`: The parent partitioned table relation from which the partition is being detached
- `name`: RangeVar specifying the partition relation to finalize detachment for

## Dependencies
- Functions called/Symbols referenced:
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md)
  - [table_openrv](../t/table_openrv.md)
  - AccessExclusiveLock
  - [WaitForOlderSnapshots](../W/WaitForOlderSnapshots.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Designed specifically for recovery scenarios where DETACH PARTITION CONCURRENTLY was interrupted
- Uses WaitForOlderSnapshots() to ensure catalog consistency before proceeding
- Always operates in concurrent mode (third parameter to DetachPartitionFinalize is true)
- Requires AccessExclusiveLock on the partition being finalized
- Returns ObjectAddress of the detached partition for further processing
- Cannot be used for initial detachment - only for completing interrupted detachments