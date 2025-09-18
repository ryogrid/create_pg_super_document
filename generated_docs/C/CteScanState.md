# CteScanState

## Location
[src/include/nodes/execnodes.h:1987-1998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1987-L1998)

## Overview
CteScanState is the execution state node for scanning Common Table Expression (CTE) queries in PostgreSQL. It manages the reading of rows from CTE queries that can be consumed by multiple readers through a shared tuplestore mechanism.

## Definition
```c
typedef struct CteScanState
{
    ScanState   ss;             /* its first field is NodeTag */
    int         eflags;         /* capability flags to pass to tuplestore */
    int         readptr;        /* index of my tuplestore read pointer */
    PlanState  *cteplanstate;   /* PlanState for the CTE query itself */
    /* Link to the "leader" CteScanState (possibly this same node) */
    struct CteScanState *leader;
    /* The remaining fields are only valid in the "leader" CteScanState */
    Tuplestorestate *cte_table; /* rows already read from the CTE query */
    bool        eof_cte;        /* reached end of CTE query? */
} CteScanState;
```

## Detailed Description
CteScanState implements the execution state for CTE scan operations. CTEs (Common Table Expressions) allow defining named subqueries that can be referenced multiple times within a query. The CteScanState uses a leader-follower pattern where multiple CteScan nodes can read from the same CTE query efficiently. A tuplestore is used to cache rows that have been read from the CTE query but not yet consumed by all readers, enabling shared access without redundant execution.

## Parameters / Member Variables
- `ss`: Base ScanState structure containing common scan node fields and NodeTag
- `eflags`: Capability flags passed to the tuplestore to control its behavior and optimization
- `readptr`: Index of this specific CteScanState tuplestore read pointer for tracking position
- `cteplanstate`: Pointer to the PlanState node that executes the actual CTE query
- `leader`: Pointer to the "leader" CteScanState node that coordinates shared access (may point to itself)
- `cte_table`: Tuplestore containing rows already read from the CTE query (only valid in leader)
- `eof_cte`: Boolean flag indicating whether the end of the CTE query has been reached (only valid in leader)

## Dependencies
- Functions called/Symbols referenced:
  - [ScanState](../S/ScanState.md)
  - Tuplestorestate
  - [PlanState](../P/PlanState.md)
- Called from (representative examples):
  - [ExecCteScan](../E/ExecCteScan.md)
  - [ExecInitCteScan](../E/ExecInitCteScan.md)
  - [ExecEndCteScan](../E/ExecEndCteScan.md)
  - [ExecReScanCteScan](../E/ExecReScanCteScan.md)
  - [CteScanNext](CteScanNext.md)
  - [CteScanRecheck](CteScanRecheck.md)

## Notes and Other Information
- Multiple CteScan nodes can read from the same CTE query through the leader-follower pattern
- The tuplestore mechanism allows efficient sharing of CTE results across multiple readers
- Only the leader CteScanState maintains the actual tuplestore and EOF status
- The readptr field allows each follower to maintain its own reading position within the shared tuplestore
- This design optimizes memory usage and execution time when CTEs are referenced multiple times