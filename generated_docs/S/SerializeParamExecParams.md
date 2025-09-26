# SerializeParamExecParams

## Location
[src/backend/executor/execParallel.c:354-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L354-L408)

## Overview
Serializes specified PARAM_EXEC parameters into shared memory for transmission to parallel worker processes in PostgreSQL's parallel query execution.

## Definition
```c
static dsa_pointer SerializeParamExecParams(EState *estate, Bitmapset *params, dsa_area *area)
```

## Detailed Description
SerializeParamExecParams creates a serialized representation of executor internal parameters in dynamic shared memory. The function follows a structured serialization format:

1. **Memory Allocation**: Uses EstimateParamExecSpace to calculate required space and allocates a contiguous block in the dynamic shared area
2. **Header Writing**: Writes the number of parameters as a 4-byte integer at the beginning
3. **Parameter Serialization**: For each parameter in the bitmapset:
   - Writes the parameter ID (4 bytes) 
   - Serializes the parameter's value and null indicator using datumSerialize
   
The function handles type information by retrieving type OID from the planned statement's paramExecTypes list and determining storage characteristics (length and by-value status). For parameters without valid type OIDs, it assumes by-value semantics consistent with other parameter handling routines.

The serialized data structure enables parallel workers to reconstruct the exact parameter values needed for query execution, maintaining data integrity across process boundaries.

## Parameters / Member Variables
- `estate`: The executor state containing parameter values and type metadata
- `params`: A bitmapset specifying which parameters to serialize
- `area`: The dynamic shared area where serialized data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - EstimateParamExecSpace
  - dsa_allocate
  - dsa_get_address
  - bms_num_members
  - bms_next_member
  - list_nth_oid
  - get_typlenbyval
  - datumSerialize
- Called from:
  - ExecInitParallelPlan
  - ExecParallelReinitialize

## Notes and Other Information
- The function returns a dsa_pointer that can be used by workers to access the serialized parameter data
- The serialization format is designed for efficient deserialization by RestoreParamExecParams
- Type OID handling follows the same logic as copyParamList for consistency
- The function is critical for parameter passing in parallel query execution scenarios
- Memory allocation is done upfront based on the estimation to avoid fragmentation
- Located in src/backend/executor/execParallel.c:354-408