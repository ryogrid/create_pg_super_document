# PushCopiedSnapshot

## Location
[src/backend/utils/time/snapmgr.c:700-711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L700-L711)

## Overview
Pushes a copy of the provided snapshot onto the active snapshot stack, ensuring the new active snapshot is modifiable.

## Definition

```c
void
PushCopiedSnapshot(Snapshot snapshot)
```
## Detailed Description
PushCopiedSnapshot creates a copy of the provided snapshot and pushes it onto the active snapshot stack. Unlike PushActiveSnapshot which may reuse existing snapshots, this function always creates a new copy, making it safe to modify the resulting active snapshot. This is particularly important when the caller needs to modify the active snapshot, such as updating the command ID with UpdateActiveSnapshotCommandId. The copied snapshot will be automatically released when it is popped from the stack.

## Parameters / Member Variables
- : The snapshot to copy and push onto the active snapshot stack

## Dependencies
- Functions called/Symbols referenced:
  - [CopySnapshot](../C/CopySnapshot.md)
  - [PushActiveSnapshot](PushActiveSnapshot.md)
- Called from (representative examples):
  - [BeginCopyTo](../B/BeginCopyTo.md)
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [refresh_matview_datafill](../r/refresh_matview_datafill.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [PortalRunMulti](PortalRunMulti.md)

## Notes and Other Information
- Always creates a copy of the snapshot, ensuring the active snapshot is modifiable
- Should be used when the caller intends to modify the active snapshot (e.g., call UpdateActiveSnapshotCommandId)
- The copied snapshot is automatically managed and released when popped from the stack
- Typically used in scenarios where command execution requires snapshot modification

## Simplified Source

```c
// Simplified version of PushCopiedSnapshot
void PushCopiedSnapshot(Snapshot snapshot) {
    // Create a copy of the snapshot and push it onto the stack
    PushActiveSnapshot(CopySnapshot(snapshot));
}
```

Key simplifications made:
- Preserved essential snapshot copying and pushing logic
- Maintained simple, clear function composition
- Focused on core functionality of making modifiable snapshots