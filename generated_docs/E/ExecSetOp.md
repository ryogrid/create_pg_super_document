# ExecSetOp

## Location
[src/backend/executor/nodeSetOp.c:190-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L190-L226)

## Overview
ExecSetOp is the main executor function for set operations (UNION, INTERSECT, EXCEPT), responsible for retrieving and returning the next tuple from a SetOp node according to the configured strategy.

## Definition

```c
static TupleTableSlot *			/* return: a tuple or NULL */
ExecSetOp(PlanState *pstate)
```
## Detailed Description
ExecSetOp implements the core execution logic for PostgreSQL's set operations. It handles two distinct strategies:

1. **Direct Strategy**: For sorted inputs, it directly processes tuples by comparing adjacent groups
2. **Hashed Strategy**: For unsorted inputs, it uses a hash table to group and count tuples

The function maintains state to handle cases where a tuple needs to be returned multiple times (for UNION ALL operations with duplicate counts). It tracks completion status through the  flag and manages output counting via .

The function follows PostgreSQL's executor pattern of returning one tuple per call, maintaining internal state between calls to track progress through the result set.

## Parameters / Member Variables
- : Pointer to the PlanState structure containing the SetOp node state and execution context

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for type casting to SetOpState)
  - CHECK_FOR_INTERRUPTS (interrupt handling macro)
  - [setop_fill_hash_table](../s/setop_fill_hash_table.md) (populates hash table for hashed strategy)
  - [setop_retrieve_hash_table](../s/setop_retrieve_hash_table.md) (retrieves next tuple from hash table)
  - [setop_retrieve_direct](../s/setop_retrieve_direct.md) (retrieves next tuple using direct comparison)
- Called from (representative examples):
  - [ExecInitSetOp](ExecInitSetOp.md) (sets this as the execution function)

## Notes and Other Information
- Returns NULL when no more tuples are available (end of result set)
- Handles duplicate output counting for operations that require multiple returns of the same tuple
- Strategy selection (SETOP_HASHED vs direct) is determined at plan time based on input characteristics
- Part of PostgreSQL's executor framework for set operations (UNION, INTERSECT, EXCEPT)

## Simplified Source

```c
static TupleTableSlot *
ExecSetOp(PlanState *pstate)
{
    SetOpState *node = castNode(SetOpState, pstate);
    SetOp *plannode = (SetOp *) node->ps.plan;
    TupleTableSlot *resultTupleSlot = node->ps.ps_ResultTupleSlot;

    CHECK_FOR_INTERRUPTS();

    // Handle duplicate output: return same tuple multiple times if needed
    if (node->numOutput > 0) {
        node->numOutput--;
        return resultTupleSlot;
    }

    // Check if processing is complete
    if (node->setop_done)
        return NULL;

    // Choose strategy based on plan configuration
    if (plannode->strategy == SETOP_HASHED) {
        // Hashed strategy: for unsorted inputs
        if (!node->table_filled)
            setop_fill_hash_table(node);  // Build hash table once
        return setop_retrieve_hash_table(node);  // Get next result from hash
    } else {
        // Direct strategy: for sorted inputs
        return setop_retrieve_direct(node);  // Compare adjacent groups directly
    }
}
```