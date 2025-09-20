# EstimateParamListSpace

## Location
[src/backend/nodes/params.c:167-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/params.c#L167-L228)

## Overview
Estimates the amount of memory space required to serialize a ParamListInfo structure for inter-process communication or storage.

## Definition

```c
structure into caller-provided storage.
 *
 * We write the number of parameters first, as a 4-byte integer, and then
 * write details for each parameter in turn.  The details for each parameter
 * consist of a 4-byte type OID, 2 bytes of flags, and then the datum as
 * serialized by datumSerialize().  The caller is responsible for ensuring
 * that there is enough storage to store the number of bytes that will be
 * written;
```
## Detailed Description
The EstimateParamListSpace function calculates the total memory space needed to serialize a ParamListInfo structure and all its parameter data. This is typically used in parallel query execution where parameter lists need to be shared between processes. The function iterates through all parameters in the list, accounting for the space needed to store each parameter's type OID, flags, and datum value. For dynamic parameters, it uses the paramFetch hook to get current values. The calculation handles both pass-by-value and pass-by-reference datatypes appropriately, using datumEstimateSpace for accurate size estimation of variable-length data.

## Parameters / Member Variables
- : The ParamListInfo structure for which to estimate serialization space. Returns minimal space (sizeof(int)) if NULL or has no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md) (safely adds sizes, checking for overflow)
  - [get_typlenbyval](../g/get_typlenbyval.md) (gets type length and pass-by-value information)
  - [datumEstimateSpace](../d/datumEstimateSpace.md) (estimates space needed for a datum value)
  - OidIsValid (macro to validate OID)
  - ParamExternData (individual parameter data type)
- Called from (representative examples):
  - [ExecInitParallelPlan](ExecInitParallelPlan.md) (in execParallel.c for parallel query setup)

## Notes and Other Information
- Returns sizeof(int) for NULL or empty parameter lists (space for parameter count)
- Handles both static and dynamic parameters through paramFetch hooks
- For parameters without valid type OIDs, assumes pass-by-value with sizeof(Datum) length
- Uses safe arithmetic functions (add_size) to prevent integer overflow
- The estimation includes space for type OID, parameter flags, and the actual datum value
- This function is essential for parallel query execution where parameters must be serialized
- The function is located in src/backend/nodes/params.c at lines 167-228