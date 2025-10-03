# jsonb_agg_transfn_worker

## Location
[src/backend/utils/adt/jsonb.c:1501-1624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1501-L1624)

## Overview
Worker function that implements the core logic for JSONB array aggregation transition functions, handling both standard and strict (absent-on-null) variants.

## Definition

```c
static Datum
jsonb_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null)
```
## Detailed Description
The  function serves as the core implementation for JSONB array aggregation transition functions. It accumulates individual values into a JSONB array during aggregate processing. The function handles the initialization of the aggregate state on first call, converts input values to JSONB format, and iterates through the JSONB structure to properly integrate elements into the growing array. It supports both standard aggregation and strict mode (where null inputs are skipped when absent_on_null is true).

## Parameters / Member Variables
- `fcinfo`: Function call information containing arguments and context
- `absent_on_null`: Boolean flag indicating whether to skip null values (true for strict aggregation)
## Dependencies
- Functions called/Symbols referenced:
  -  - Verify aggregate function context
  -  - Get argument type information
  - ,  - Memory allocation functions
  -  - Add values to JSONB structure
  -  - Determine JSON type category
  -  - Convert datum to JSONB
  -  - Convert JsonbValue to final JSONB
  - ,  - JSONB iteration functions
  - ,  - [Numeric](../N/Numeric.md) value copying
  - Memory context functions: 
  - Constants: , , , , , , , 
- Called from:
  -  (src/backend/utils/adt/jsonb.c:1627)
  -  (src/backend/utils/adt/jsonb.c:1636)

## Notes and Other Information
- Handles both initialization (first call with null state) and accumulation phases
- Manages memory contexts properly for aggregate operations
- Special handling for scalar arrays to avoid double-wrapping
- Copies string and numeric values into the aggregate memory context for persistence
- Validates that it's called within proper aggregate context
- Supports conditional null handling based on absent_on_null parameter
- Uses JsonbIterator to traverse complex JSONB structures element by element