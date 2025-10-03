# ExecRestrPos

## Location
[src/backend/executor/execAmi.c:375-416](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execAmi.c#L375-L416)

## Overview
ExecRestrPos restores the scan position that was previously saved with ExecMarkPos, ensuring that subsequent tuple retrieval continues from the marked position.

## Definition

```c
void
ExecRestrPos(PlanState *node)
```
## Detailed Description
ExecRestrPos is the counterpart to ExecMarkPos, restoring a plan node's scan position to a location that was previously marked. This function is critical for implementing algorithms that need to re-read portions of input data, particularly in MergeJoin operations where duplicate values may require backing up and re-scanning sections of the inner relation.

The function provides strict semantic guarantees: the first ExecProcNode call following a restore operation will yield the same tuple that was returned by the first ExecProcNode call following the corresponding mark operation. This ensures predictable and repeatable access to data streams.

Like ExecMarkPos, this function operates as a dispatcher that delegates to node-type-specific restore functions:

- **IndexScanState**: Restores position within index scans
- **IndexOnlyScanState**: Restores position within index-only scans
- **CustomScanState**: Delegates to custom scan provider implementations  
- **MaterialState**: Restores position within materialized results
- **SortState**: Restores position within sorted data
- **ResultState**: Restores position within result nodes

Unlike ExecMarkPos, this function throws a hard error for unsupported node types, since attempting to restore to a non-existent mark position represents a programming error that should be caught.

## Parameters / Member Variables
- `*node`: Pointer to the PlanState node where the position should be restored. Must be of a type that supports mark/restore operations and must have had ExecMarkPos called on it previously.
## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (node type identification)
  - [ExecIndexRestrPos](ExecIndexRestrPos.md) (index scan position restoration)
  - [ExecIndexOnlyRestrPos](ExecIndexOnlyRestrPos.md) (index-only scan position restoration)  
  - [ExecCustomRestrPos](ExecCustomRestrPos.md) (custom scan position restoration)
  - [ExecMaterialRestrPos](ExecMaterialRestrPos.md) (material node position restoration)
  - [ExecSortRestrPos](ExecSortRestrPos.md) (sort node position restoration)
  - [ExecResultRestrPos](ExecResultRestrPos.md) (result node position restoration)
- Called from (representative examples):
  - [ExecMergeJoin](ExecMergeJoin.md) (merge join operations)
  - [ExecResultRestrPos](ExecResultRestrPos.md) (result node delegation)

## Notes and Other Information
- The function provides strong semantic guarantees about tuple retrieval consistency after restore
- The state of the result TupleTableSlot after restore is unspecified - it may be unchanged, cleared, or loaded with the restored-to tuple
- Callers should discard any previously returned TupleTableSlot after performing a restore operation
- Unlike ExecMarkPos, this function throws a hard ERROR for unrecognized node types since restore without a valid mark represents a programming error
- The mark/restore mechanism is primarily used by MergeJoin when processing duplicate values requires backing up in the input stream
- Each node type maintains its own internal position tracking mechanism to support the restore functionality

## Simplified Source

```c
void
ExecRestrPos(PlanState *node)
{
    // Dispatch to appropriate restore function based on node type
    switch (nodeTag(node)) {
        case T_IndexScanState:
            ExecIndexRestrPos((IndexScanState *) node);
            break;

        case T_IndexOnlyScanState:
            ExecIndexOnlyRestrPos((IndexOnlyScanState *) node);
            break;

        case T_CustomScanState:
            ExecCustomRestrPos((CustomScanState *) node);
            break;

        case T_MaterialState:
            ExecMaterialRestrPos((MaterialState *) node);
            break;

        case T_SortState:
            ExecSortRestrPos((SortState *) node);
            break;

        case T_ResultState:
            ExecResultRestrPos((ResultState *) node);
            break;

        default:
            // Error for unsupported node types
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
            break;
    }
}
```