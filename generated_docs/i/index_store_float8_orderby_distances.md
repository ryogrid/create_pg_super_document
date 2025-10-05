# index_store_float8_orderby_distances

## Location
[src/backend/access/index/indexam.c:928-995](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L928-L995)

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
  - [Float8GetDatum](../F/Float8GetDatum.md)
  - [Float4GetDatum](../F/Float4GetDatum.md)
  - [DatumGetPointer](../D/DatumGetPointer.md) (via pfree)
- Called from (representative examples):
  - [getNextNearest](../g/getNextNearest.md) (GiST nearest-neighbor search)
  - [spggettuple](../s/spggettuple.md) (SP-GiST tuple retrieval)

## Notes and Other Information
- Manages memory properly by freeing old float8 values when USE_FLOAT8_BYVAL is not defined
- Only supports conversion to float8 and float4 ORDER BY types; other types result in NULL unless recheck is required
- The recheck mechanism allows for handling of lossy distance calculations where exact ordering verification is needed
- Essential for implementing nearest-neighbor queries and distance-based ordering in spatial and other specialized indexes

## Simplified Source

```c
void
index_store_float8_orderby_distances(IndexScanDesc scan, Oid *orderByTypes,
                                     IndexOrderByDistance *distances,
                                     bool recheckOrderBy)
{
    Assert(distances || !recheckOrderBy);
    scan->xs_recheckorderby = recheckOrderBy;

    // Process each ORDER BY expression
    for (int i = 0; i < scan->numberOfOrderBys; i++)
    {
        if (orderByTypes[i] == FLOAT8OID)
        {
            // Handle float8 ORDER BY type
#ifndef USE_FLOAT8_BYVAL
            if (!scan->xs_orderbynulls[i])
                pfree(DatumGetPointer(scan->xs_orderbyvals[i]));  // Free old value
#endif
            if (distances && !distances[i].isnull)
            {
                scan->xs_orderbyvals[i] = Float8GetDatum(distances[i].value);
                scan->xs_orderbynulls[i] = false;
            }
            else
            {
                scan->xs_orderbyvals[i] = (Datum) 0;
                scan->xs_orderbynulls[i] = true;
            }
        }
        else if (orderByTypes[i] == FLOAT4OID)
        {
            // Handle float4 ORDER BY type - convert from float8
            if (distances && !distances[i].isnull)
            {
                scan->xs_orderbyvals[i] = Float4GetDatum((float4) distances[i].value);
                scan->xs_orderbynulls[i] = false;
            }
            else
            {
                scan->xs_orderbyvals[i] = (Datum) 0;
                scan->xs_orderbynulls[i] = true;
            }
        }
        else
        {
            // For other types, only NULL values are supported
            if (scan->xs_recheckorderby)
                elog(ERROR, "ORDER BY operator must return float8 or float4 if the distance function is lossy");
            scan->xs_orderbynulls[i] = true;
        }
    }
}
```