# ExecInitNamedTuplestoreScan

## Location
[src/backend/executor/nodeNamedtuplestorescan.c:82-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeNamedtuplestorescan.c#L82-L163)

## Overview
Initializes a NamedTuplestoreScan plan node, setting up the scan state, tuple store access, and all necessary execution context for scanning CTE data from named tuple stores.

## Definition
```c
NamedTuplestoreScanState *
ExecInitNamedTuplestoreScan(NamedTuplestoreScan *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitNamedTuplestoreScan performs comprehensive initialization of a NamedTuplestoreScan plan node, creating and configuring the corresponding NamedTuplestoreScanState structure. The function first validates that unsupported execution flags (backward scanning and mark/restore) are not set, then locates the named tuple store via the Ephemeral Named Relation (ENR) mechanism using the provided relation name.

The function allocates a new read pointer for the tuple store and positions it at the beginning via explicit rewind. It sets up the scan tuple slot using the tuple descriptor from the ENR metadata and initializes result type handling and projection information. Finally, it initializes any qualification expressions that may need to be evaluated during the scan.

The initialization follows PostgreSQL's standard executor initialization pattern, setting up expression contexts, tuple slots, projection info, and qualification expressions. The function ensures the scan state is fully prepared for execution by ExecNamedTuplestoreScan.

## Parameters / Member Variables
- `node`: Pointer to NamedTuplestoreScan plan node containing the scan configuration and ENR name
- `estate`: Execution state containing query environment and execution context
- `eflags`: Execution flags controlling scan behavior (backward scanning and mark operations not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Creates new NamedTuplestoreScanState node
  - [ExecNamedTuplestoreScan](ExecNamedTuplestoreScan.md): Sets as the main execution function
  - [get_ENR](../g/get_ENR.md): Retrieves Ephemeral Named Relation by name from query environment
  - [ENRMetadataGetTupDesc](ENRMetadataGetTupDesc.md): Gets tuple descriptor from ENR metadata
  - [tuplestore_alloc_read_pointer](../t/tuplestore_alloc_read_pointer.md): Allocates new read pointer for tuple store access
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md): Selects the read pointer for operations
  - [tuplestore_rescan](../t/tuplestore_rescan.md): Rewinds the tuple store to beginning
  - [ExecAssignExprContext](ExecAssignExprContext.md): Creates expression evaluation context
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md): Initializes scan tuple slot with tuple descriptor
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md): Initializes result tuple type from target list
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md): Sets up projection information
  - [ExecInitQual](ExecInitQual.md): Initializes qualification expressions
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md): Generic node initialization dispatcher in executor framework

## Notes and Other Information
- Function validates that the plan node has no child nodes (outer/inner plans must be NULL)
- Does not support EXEC_FLAG_BACKWARD or EXEC_FLAG_MARK execution flags
- Must find a valid ENR with the specified name or throws an error
- Allocates a read pointer with EXEC_FLAG_REWIND capability and explicitly rewinds it
- Uses TTSOpsMinimalTuple for scan tuple slot operations
- Comment indicates that attempts to add read pointer cleanup did not improve performance
- Part of the CTE (Common Table Expression) execution infrastructure
- Returns fully initialized NamedTuplestoreScanState ready for execution