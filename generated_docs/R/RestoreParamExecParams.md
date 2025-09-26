# RestoreParamExecParams

## Location
[src/backend/executor/execParallel.c:409-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L409-L437)

## Overview
Deserializes PARAM_EXEC parameters from shared memory and restores them into a parallel worker's executor state.

## Definition
```c
static void RestoreParamExecParams(char *start_address, EState *estate)
```

## Detailed Description
RestoreParamExecParams reconstructs executor internal parameters from their serialized representation in shared memory. This function is the counterpart to SerializeParamExecParams and follows the same data format:

1. **Header Reading**: Reads the number of parameters from the first 4 bytes
2. **Parameter Restoration**: For each parameter:
   - Reads the parameter ID (4 bytes)
   - Uses datumRestore to deserialize the parameter's value and null indicator
   - Updates the corresponding entry in the executor state's parameter array
   - Sets execPlan to NULL since worker processes don't execute parameter subplans

The function enables parallel workers to access parameter values that were computed by the leader process or other workers, ensuring consistent parameter state across all processes involved in parallel query execution.

This restoration process is essential for maintaining data consistency and enabling proper query execution in the parallel worker context.

## Parameters / Member Variables
- `start_address`: Pointer to the beginning of serialized parameter data in shared memory
- `estate`: The executor state where restored parameters will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [datumRestore](../d/datumRestore.md)
  - [ParamExecData](../P/ParamExecData.md) (struct)
- Called from:
  - [ParallelQueryMain](../P/ParallelQueryMain.md)

## Notes and Other Information
- The function assumes the serialized data follows the exact format created by SerializeParamExecParams
- Parameter execPlan is explicitly set to NULL since workers don't execute parameter subplans
- The function operates directly on the estate's es_param_exec_vals array
- Memory layout parsing relies on the fixed-size integer headers for parameter count and IDs
- This function is called during parallel worker initialization to establish the parameter context
- Located in src/backend/executor/execParallel.c:409-437