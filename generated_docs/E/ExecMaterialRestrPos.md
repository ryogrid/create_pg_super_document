# ExecMaterialRestrPos

## Location
[src/backend/executor/nodeMaterial.c:290-312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMaterial.c#L290-L312)

## Overview
ExecMaterialRestrPos restores the tuplestore read position to the previously marked position, completing the mark/restore functionality for Material nodes.

## Definition

```c
void
ExecMaterialRestrPos(MaterialState *node)
```
## Detailed Description
ExecMaterialRestrPos implements the restore operation for Material nodes by copying the mark pointer position back to the active read pointer in the tuplestore. This allows the executor to return to a previously saved position that was set by ExecMaterialMarkPos, enabling efficient backtracking during query execution.

The function is the counterpart to ExecMaterialMarkPos and provides the restore half of the mark/restore functionality. It includes the same safety checks as the mark function, ensuring that mark/restore was properly configured during initialization and handling the case where materialization hasn't occurred yet.

## Parameters / Member Variables
- `*node`: The MaterialState node for which to restore the marked position
## Dependencies
- Functions called/Symbols referenced:
  - [MaterialState](../M/MaterialState.md)
  - EXEC_FLAG_MARK
  - [tuplestore_copy_read_pointer](../t/tuplestore_copy_read_pointer.md)
- Called from (representative examples):
  - [ExecRestrPos](ExecRestrPos.md)

## Notes and Other Information
- Requires EXEC_FLAG_MARK to be set during node initialization, same as ExecMaterialMarkPos
- Copies from read pointer 1 (mark) to read pointer 0 (active), opposite of ExecMaterialMarkPos
- Returns immediately if no tuplestore has been created yet (consistent with lazy initialization)
- Does not perform tuplestore_trim like the mark function since restoring doesn't advance any positions
- The restore operation can be called multiple times to return to the same marked position

## Simplified Source

```c
void ExecMaterialRestrPos(MaterialState *node)
{
    // Verify mark/restore capability was requested
    Assert(node->eflags & EXEC_FLAG_MARK);

    // Skip if tuplestore not materialized yet
    if (!node->tuplestorestate)
        return;

    // Copy mark position back to active read pointer
    tuplestore_copy_read_pointer(node->tuplestorestate, 1, 0);
}
```