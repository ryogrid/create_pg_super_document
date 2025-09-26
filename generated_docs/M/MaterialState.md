# MaterialState

## Location
[src/include/nodes/execnodes.h:2226-2232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2226-L2232)

## Overview
MaterialState is the execution state structure for materialization nodes in PostgreSQL, which store the results of a subplan into a temporary file for repeated access.

## Definition
```c
typedef struct MaterialState
{
    ScanState   ss;                 /* its first field is NodeTag */
    int         eflags;             /* capability flags to pass to tuplestore */
    bool        eof_underlying;     /* reached end of underlying plan? */
    Tuplestorestate *tuplestorestate;
} MaterialState;
```

## Detailed Description
MaterialState represents the execution state for a materialization node, which is used to cache the output of a subplan into a temporary storage structure called a tuplestore. This allows the results to be read multiple times without re-executing the underlying plan. The materialization node is particularly useful when the same data needs to be accessed repeatedly, such as in certain join algorithms, window functions, or when multiple scans of the same subquery result are required.

The node operates in two phases: first, it reads all tuples from the underlying plan and stores them in the tuplestore; then, it can rescan the stored tuples multiple times efficiently. The tuplestore can be configured with different capabilities (like backward scanning, mark/restore functionality) depending on the requirements of the parent node.

## Parameters / Member Variables
- `ss`: Base ScanState structure containing common scan execution state including tuple slot management
- `eflags`: Capability flags passed to tuplestore indicating required features (EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, etc.)
- `eof_underlying`: Boolean flag indicating whether all tuples from the underlying plan have been read and stored
- `tuplestorestate`: Pointer to the tuplestore structure that holds the materialized tuples

## Dependencies
- Functions called/Symbols referenced:
  - [ScanState](../S/ScanState.md) (base scan execution state)
  - [Tuplestorestate](../T/Tuplestorestate.md) (temporary tuple storage structure)
- Called from (representative examples):
  - [ExecMaterial](../E/ExecMaterial.md)
  - [ExecInitMaterial](../E/ExecInitMaterial.md)
  - [ExecEndMaterial](../E/ExecEndMaterial.md)
  - [ExecMaterialMarkPos](../E/ExecMaterialMarkPos.md)
  - [ExecMaterialRestrPos](../E/ExecMaterialRestrPos.md)

## Notes and Other Information
- Materialization is used when plans need to be rescanned but the underlying plan does not support efficient rescanning
- The tuplestore can optionally support backward scanning and mark/restore operations based on parent node requirements
- Memory usage is managed through the tuplestore, which can spill to disk if the data exceeds work_mem
- The node inherits from ScanState, making it compatible with generic scan processing functions
- ss.ss_ScanTupleSlot refers to the output of the underlying plan during the initial materialization phase
- Once materialization is complete, subsequent reads come from the tuplestore rather than the underlying plan