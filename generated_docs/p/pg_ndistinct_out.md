# pg_ndistinct_out

## Location
[src/backend/statistics/mvdistinct.c:355-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L355-L391)

## Overview
A PostgreSQL output function for the pg_ndistinct data type that produces a human-readable string representation of multivariate n-distinct statistics.

## Definition

```c
Datum
pg_ndistinct_out(PG_FUNCTION_ARGS)
```
## Detailed Description
The pg_ndistinct_out function serves as the output routine for the pg_ndistinct data type in PostgreSQL. It takes serialized multivariate n-distinct statistics data and converts it into a human-readable JSON-like string format. The function deserializes the input bytea data into an MVNDistinct structure and iterates through all n-distinct items to format them as a string containing attribute combinations and their corresponding n-distinct values.

The output format is a JSON-like structure where each entry shows the attribute numbers involved in the n-distinct calculation followed by the calculated n-distinct value.

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS):

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (macro to get bytea input parameter)
  - [statext_ndistinct_deserialize](../s/statext_ndistinct_deserialize.md) (deserializes bytea to MVNDistinct structure)
  - [initStringInfo](../i/initStringInfo.md) (initializes string buffer)
  - [appendStringInfoChar](../a/appendStringInfoChar.md) (appends character to string buffer)
  - [appendStringInfoString](../a/appendStringInfoString.md) (appends string to string buffer)  
  - [appendStringInfo](../a/appendStringInfo.md) (appends formatted string to buffer)
  - PG_RETURN_CSTRING (macro to return C string as Datum)
- Data types used:
  - [MVNDistinct](../M/MVNDistinct.md) (multivariate n-distinct statistics structure)
  - MVNDistinctItem (individual n-distinct item structure)
  - AttrNumber (attribute number type)
- Called from:
  - No direct references found (used as type output function)

## Notes and Other Information
- This function is registered as the output function for the pg_ndistinct data type
- The output format resembles JSON with attribute combinations as keys and n-distinct values as values
- The function handles multiple attribute combinations within a single n-distinct statistics object
- Located in src/backend/statistics/mvdistinct.c:355-391