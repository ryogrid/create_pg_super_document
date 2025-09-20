# ExecReScanLimit

## Location
[src/backend/executor/nodeLimit.c:541-558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLimit.c#L541-L558)

## Overview
ExecReScanLimit reinitializes a LIMIT node for rescanning, recomputing limit/offset expressions and resetting internal state before potentially rescanning the child node.

## Definition

```c
void
ExecReScanLimit(LimitState *node)
```
## Detailed Description
ExecReScanLimit is the rescan method for LIMIT execution nodes in PostgreSQL's executor. When a query plan needs to be rescanned (such as in nested loops or when parameters change), this function prepares the LIMIT node for a fresh scan by:

1. Recomputing the LIMIT and OFFSET expressions, which is essential because parameters may have changed since the initial execution
2. Resetting the internal state machine of the LIMIT node
3. Conditionally rescanning the child node only if it doesn't have changed parameters (chgParam == NULL)

The function ensures proper ordering by recomputing limits before rescanning the child node, which is particularly important when the child is a Sort node that needs to know the parameter values.

## Parameters / Member Variables
- : Pointer to the LimitState structure representing the LIMIT node being rescanned
  - Contains the plan state, limit/offset expressions, current values, and state machine information

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the outer plan state from the LIMIT node
  - : Recomputes LIMIT and OFFSET values and resets state machine
  - : Recursively rescans the child node if needed
- Called from:
  -  (src/backend/executor/execAmi.c:297): Part of the general executor rescan mechanism

## Notes and Other Information
- The function follows a careful ordering: limit recomputation must happen before child node rescanning to ensure Sort nodes receive updated parameters
- The conditional rescanning logic (checking chgParam == NULL) is an optimization - if the child node has changed parameters, it will be automatically rescanned on the first ExecProcNode call
- This function is part of PostgreSQL's executor node interface, specifically handling the LIMIT clause in SQL queries
- The recompute_limits() call handles both LIMIT and OFFSET expressions, validates non-negative values, and resets the position tracking and state machine to LIMIT_RESCAN state