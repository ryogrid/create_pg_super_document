# ExecHash

## Location
[src/backend/executor/nodeHash.c:91-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L91-L104)

## Overview
ExecHash is a stub function for pro forma compliance that serves as a placeholder in the Hash node's execution interface but is not intended to be called during normal operation.

## Definition

```c
static TupleTableSlot *
ExecHash(PlanState *pstate)
```
## Detailed Description
ExecHash is a static function that exists purely for interface compliance within PostgreSQL's executor framework. The function intentionally throws an error when called, as Hash nodes do not support the standard ExecProcNode call convention. This is because Hash nodes are special-purpose nodes that are only used to build hash tables for hash joins, and they are executed through the MultiExecHash interface rather than the standard tuple-by-tuple execution model used by other plan nodes.

The Hash node's execution model is fundamentally different from other executor nodes - instead of producing tuples one at a time, it consumes all input tuples to build a complete hash table that will be used by its parent HashJoin node.

## Parameters / Member Variables
- : PlanState pointer (unused, as function immediately errors)

## Dependencies
- Functions called/Symbols referenced:
  - elog
- Called from (representative examples):
  - [ExecInitHash](ExecInitHash.md) (sets this as the ExecProcNode function pointer)

## Notes and Other Information
- This function should never actually be executed during normal query processing
- The real Hash node execution happens through MultiExecHash, which is called by the parent HashJoin node
- The error message "Hash node does not support ExecProcNode call convention" clearly indicates the intended usage pattern
- Located in src/backend/executor/nodeHash.c:91-104

## Simplified Source

```c
// Simplified version of ExecHash
static TupleTableSlot *
ExecHash(PlanState *pstate)
{
    // Hash nodes don't support standard tuple-by-tuple execution
    // They use MultiExecHash instead for building complete hash tables
    elog(ERROR, "Hash node does not support ExecProcNode call convention");
    return NULL;
}
```