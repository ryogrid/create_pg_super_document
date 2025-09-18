# gist_point_consistent

## Location
src/backend/access/gist/gistproc.c: 1337 - 1454

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
  - `gist_point_consistent_internal`
  - `gist_poly_consistent`
  - `gist_circle_consistent`
  - `DatumGetBoxP`
  - `GIST_LEAF`
  - `DirectFunctionCall5`
  - `DirectFunctionCall2`
  - `poly_contain_pt`
  - `circle_contain_pt`
- Called from (representative examples):
  - GiST index access methods (indirectly through function pointers)

## Notes and Other Information
- Remaps legacy strategy numbers (`RTOldBelowStrategyNumber`, `RTOldAboveStrategyNumber`) for backward compatibility
- Uses strategy groups to categorize different geometric query types via `GeoStrategyNumberOffset`
- For box containment queries, implements exact (non-fuzzy) overlap testing to avoid unnecessary child page visits
- On leaf pages, performs exact containment tests for polygon and circle queries after initial bounding box checks
- Sets `*recheck = false` for most operations as they provide definitive results