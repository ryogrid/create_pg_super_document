# EstimateParamExecSpace

## Location
[src/backend/executor/execParallel.c:310-353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L310-L353)

## Overview
Calculates the amount of shared memory space required to serialize a set of PARAM_EXEC parameters for transmission to parallel worker processes.

## Definition
```c
static Size EstimateParamExecSpace(EState *estate, Bitmapset *params)
```

## Detailed Description
EstimateParamExecSpace computes the serialization space requirements for a collection of executor internal parameters that need to be shared with parallel workers. The function iterates through each parameter specified in the input bitmapset and calculates the space needed based on:

1. **Header Space**: Includes space for the parameter count (4 bytes)
2. **Per-Parameter Overhead**: Each parameter requires space for its parameter ID (4 bytes)
3. **Data Space**: The actual space needed for the parameter's value, calculated using datumEstimateSpace based on the parameter's data type characteristics

The function handles type information by looking up the parameter's type OID from the planned statement's paramExecTypes list. For parameters without valid type OIDs, it assumes by-value semantics with Datum-sized storage, following the same logic used by copyParamList.

The space calculation accounts for variable-length data types and ensures accurate memory allocation for the subsequent serialization process.

## Parameters / Member Variables
- `estate`: The executor state containing parameter values and type information
- `params`: A bitmapset indicating which parameters need space estimation

## Dependencies
- Functions called/Symbols referenced:
  - bms_next_member
  - list_nth_oid  
  - add_size
  - get_typlenbyval
  - datumEstimateSpace
- Called from:
  - SerializeParamExecParams

## Notes and Other Information
- The function assumes by-value semantics for parameters without valid type OIDs, consistent with other parameter handling code
- Space estimation is crucial for proper dynamic shared area allocation in parallel query execution
- The function works with PostgreSQL's bitmapset data structure for efficient parameter set iteration
- Type length and by-value information is cached per parameter to optimize the estimation process
- Located in src/backend/executor/execParallel.c:310-353