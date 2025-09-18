# AttachIndexCallbackState

## Location
src/backend/commands/tablecmds.c: 19787 - 19794

## Overview
A state structure used during index partition attachment operations to coordinate proper locking order between index and table objects, preventing deadlocks during ALTER INDEX ATTACH PARTITION commands.

## Definition
```c
struct AttachIndexCallbackState
{
    Oid         partitionOid;
    Oid         parentTblOid;
    bool        lockedParentTbl;
};
```

## Detailed Description
This structure manages the locking state during the execution of `ALTER INDEX ATTACH PARTITION` operations. It ensures that locks are acquired in the correct order to prevent deadlocks when attaching a partition index to a partitioned index. The structure coordinates between `ATExecAttachPartitionIdx` and its callback function `RangeVarCallbackForAttachIndex` to maintain proper lock acquisition protocol.

The key principle is that table locks must be acquired before their corresponding index locks to avoid deadlock situations. This structure tracks which locks have been acquired and manages the OIDs of the relevant objects throughout the attachment process.

## Parameters / Member Variables
- `partitionOid`: OID of the partition table that owns the index being attached (set by the callback)
- `parentTblOid`: OID of the parent partitioned table that owns the target partitioned index (set by caller)
- `lockedParentTbl`: Boolean flag indicating whether AccessShareLock has been acquired on the parent table

## Dependencies
- Functions called/Symbols referenced:
  - Oid (data type)
  - [bool](../b/bool.md) (data type)
- Called from (representative examples):
  - [ATExecAttachPartitionIdx](ATExecAttachPartitionIdx.md) (src/backend/commands/tablecmds.c:19857)
  - [RangeVarCallbackForAttachIndex](../R/RangeVarCallbackForAttachIndex.md) (src/backend/commands/tablecmds.c:19798, 19802)

## Notes and Other Information
- Critical for maintaining proper lock acquisition order during index partition attachment
- Prevents deadlocks by ensuring parent table locks are acquired before partition table and index locks
- The callback function uses this state to track and manage subsidiary locks on related objects
- Used specifically in the context of partitioned index operations where multiple objects must be locked in coordination
- The `lockedParentTbl` flag prevents redundant lock acquisition attempts
- Part of PostgreSQL's table partitioning infrastructure for safe DDL operations on partitioned objects