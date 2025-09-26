# CheckPointStmt

## Location
[src/include/nodes/parsenodes.h:3914-3917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3914-L3917)

## Overview
CheckPointStmt represents the parsed structure of a CHECKPOINT statement, which forces an immediate checkpoint operation in PostgreSQL.

## Definition

```c
typedef struct CheckPointStmt
{
	NodeTag		type;
} CheckPointStmt;
```
## Detailed Description
CheckPointStmt is a simple parse node representing the CHECKPOINT SQL statement. Unlike many other statement types, it contains only the basic NodeTag since the CHECKPOINT command has no parameters or options in standard SQL. When executed, this statement triggers PostgreSQL's checkpoint mechanism, which forces all dirty shared buffers to be written to disk and updates the control file. This is primarily used for administrative purposes and testing.

## Parameters / Member Variables
- : NodeTag identifying this as a CheckPointStmt node

## Dependencies
- Functions called/Symbols referenced:
  - None (only contains NodeTag)
- Called from (representative examples):
  - PlannedStmtRequiresSnapshot

## Notes and Other Information
- Represents the simplest possible SQL statement structure with only a NodeTag
- The CHECKPOINT command is typically used by database administrators for maintenance operations
- Executing a checkpoint forces all dirty pages in shared memory to be written to persistent storage
- Unlike automatic checkpoints, manual CHECKPOINT commands provide immediate control over when expensive I/O operations occur
- Often used in backup procedures and before major maintenance operations
- The actual checkpoint functionality is implemented in the checkpoint subsystem, not in this parse node structure