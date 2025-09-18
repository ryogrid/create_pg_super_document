# index_store_float8_orderby_distances

## Location
src/backend/access/index/indexam.c: 928 - 995

## Overview
Converts access method distance function results to ORDER BY data types and stores them in the index scan descriptor for potential recheck operations.

## Definition
```c
void
index_store_float8_orderby_distances(IndexScanDesc scan, Oid *orderByTypes,
                                     IndexOrderByDistance *distances,
                                     bool recheckOrderBy)
```

## Detailed Description
This function handles the conversion and storage of distance values computed by index access method distance functions into the appropriate ORDER BY data types. Distance functions typically return float8 values that may be inexact, requiring potential rechecking during query execution. The function iterates through each ORDER BY expression, converting distance values to the expected type (float8 or float4) and storing them in the scan descriptor's xs_orderbyvals and xs_orderbynulls arrays.

For float8 ORDER BY types, the function directly stores the distance value. For float4 types, it performs a cast from float8 to float4. For other data types, the function can only store NULL values unless recheck is required, in which case it raises an error since conversion from float8 to arbitrary types is not supported.

## Parameters / Member Variables
- `scan`: Index scan descriptor where the converted values will be stored
- `orderByTypes`: Array of OIDs representing the data types expected by ORDER BY expressions
- `distances`: Array of IndexOrderByDistance structures containing computed distance values from the AM
- `recheckOrderBy`: Boolean flag indicating whether recheck operations may be needed

## Dependencies
- Functions called/Symbols referenced:
  - Float8GetDatum
  - Float4GetDatum
  - DatumGetPointer (via pfree)
- Called from (representative examples):
  - getNextNearest (GiST nearest-neighbor search)
  - spggettuple (SP-GiST tuple retrieval)

## Notes and Other Information
- Manages memory properly by freeing old float8 values when USE_FLOAT8_BYVAL is not defined
- Only supports conversion to float8 and float4 ORDER BY types; other types result in NULL unless recheck is required
- The recheck mechanism allows for handling of lossy distance calculations where exact ordering verification is needed
- Essential for implementing nearest-neighbor queries and distance-based ordering in spatial and other specialized indexes