# gist_point_consistent

## Location
[src/backend/access/gist/gistproc.c:1337-1454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L1337-L1454)

## Overview
Implements the GiST consistent method for point data types, determining whether a point query matches entries in a GiST index by handling various geometric query types including point-to-point, point-in-box, point-in-polygon, and point-in-circle operations.

## Definition
```c
Datum gist_point_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the main entry point for GiST consistency checking for point data types. It processes different types of spatial queries by categorizing them into strategy groups and delegating to appropriate handlers. The function handles legacy strategy number remapping for backward compatibility and supports multiple geometric query types:

1. **Point-to-point operations**: Uses `gist_point_consistent_internal` for spatial relationships
2. **Point-in-box operations**: Performs exact (non-fuzzy) overlap testing for containment queries
3. **Point-in-polygon operations**: Delegates to `gist_poly_consistent` and performs exact containment testing on leaf pages
4. **Point-in-circle operations**: Delegates to `gist_circle_consistent` and performs exact containment testing on leaf pages

The function implements strategy-specific logic, with special handling for leaf vs. internal nodes, and sets the recheck flag appropriately based on the query type and result.

## Parameters / Member Variables
- `entry`: GiST entry containing the indexed point data and metadata
- `strategy`: Strategy number indicating the type of spatial operation to perform
- `recheck`: Output parameter indicating whether the result needs to be rechecked at higher levels

## Dependencies
- Functions called/Symbols referenced:
  - [gist_point_consistent_internal](gist_point_consistent_internal.md)
  - [gist_poly_consistent](gist_poly_consistent.md)
  - [gist_circle_consistent](gist_circle_consistent.md)
  - [DatumGetBoxP](../D/DatumGetBoxP.md)
  - `GIST_LEAF`
  - `DirectFunctionCall5`
  - `DirectFunctionCall2`
  - [poly_contain_pt](../p/poly_contain_pt.md)
  - [circle_contain_pt](../c/circle_contain_pt.md)
- Called from (representative examples):
  - GiST index access methods (indirectly through function pointers)

## Notes and Other Information
- Remaps legacy strategy numbers (`RTOldBelowStrategyNumber`, `RTOldAboveStrategyNumber`) for backward compatibility
- Uses strategy groups to categorize different geometric query types via `GeoStrategyNumberOffset`
- For box containment queries, implements exact (non-fuzzy) overlap testing to avoid unnecessary child page visits
- On leaf pages, performs exact containment tests for polygon and circle queries after initial bounding box checks
- Sets `*recheck = false` for most operations as they provide definitive results

## Simplified Source

```c
Datum gist_point_consistent(PG_FUNCTION_ARGS) {
    GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);
    bool *recheck = (bool *) PG_GETARG_POINTER(4);
    bool result;
    StrategyNumber strategyGroup;

    // Remap legacy strategy numbers for backward compatibility
    if (strategy == RTOldBelowStrategyNumber)
        strategy = RTBelowStrategyNumber;
    else if (strategy == RTOldAboveStrategyNumber)
        strategy = RTAboveStrategyNumber;

    // Determine strategy group for dispatch
    strategyGroup = strategy / GeoStrategyNumberOffset;

    switch (strategyGroup) {
        case PointStrategyNumberGroup:
            // Point-to-point operations
            result = gist_point_consistent_internal(
                strategy % GeoStrategyNumberOffset,
                GIST_LEAF(entry),
                DatumGetBoxP(entry->key),
                PG_GETARG_POINT_P(1));
            *recheck = false;
            break;

        case BoxStrategyNumberGroup:
            // Point-in-box containment test (exact, non-fuzzy)
            {
                BOX *query = PG_GETARG_BOX_P(1);
                BOX *key = DatumGetBoxP(entry->key);

                result = (key->high.x >= query->low.x &&
                         key->low.x <= query->high.x &&
                         key->high.y >= query->low.y &&
                         key->low.y <= query->high.y);
                *recheck = false;
            }
            break;

        case PolygonStrategyNumberGroup:
            // Point-in-polygon operations
            {
                POLYGON *query = PG_GETARG_POLYGON_P(1);

                // Initial bounding box check
                result = DatumGetBool(DirectFunctionCall5(gist_poly_consistent,
                    PointerGetDatum(entry), PolygonPGetDatum(query),
                    Int16GetDatum(RTOverlapStrategyNumber), 0,
                    PointerGetDatum(recheck)));

                // Exact containment test on leaf pages
                if (GIST_LEAF(entry) && result) {
                    BOX *box = DatumGetBoxP(entry->key);
                    result = DatumGetBool(DirectFunctionCall2(poly_contain_pt,
                        PolygonPGetDatum(query),
                        PointPGetDatum(&box->high)));
                    *recheck = false;
                }
            }
            break;

        case CircleStrategyNumberGroup:
            // Point-in-circle operations
            {
                CIRCLE *query = PG_GETARG_CIRCLE_P(1);

                // Initial bounding box check
                result = DatumGetBool(DirectFunctionCall5(gist_circle_consistent,
                    PointerGetDatum(entry), CirclePGetDatum(query),
                    Int16GetDatum(RTOverlapStrategyNumber), 0,
                    PointerGetDatum(recheck)));

                // Exact containment test on leaf pages
                if (GIST_LEAF(entry) && result) {
                    BOX *box = DatumGetBoxP(entry->key);
                    result = DatumGetBool(DirectFunctionCall2(circle_contain_pt,
                        CirclePGetDatum(query),
                        PointPGetDatum(&box->high)));
                    *recheck = false;
                }
            }
            break;

        default:
            elog(ERROR, "unrecognized strategy number: %d", strategy);
            result = false;
            break;
    }

    PG_RETURN_BOOL(result);
}
```