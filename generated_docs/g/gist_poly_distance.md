# gist_poly_distance

## Location
src/backend/access/gist/gistproc.c: 1543 - 1574

## Overview
A GiST distance function for polygon data types that calculates the distance from a query point to a polygon index entry during nearest-neighbor searches.

## Definition


## Detailed Description
This function serves as a GiST distance operator for polygon data types, implementing the distance calculation interface required by GiST nearest-neighbor queries. The function acts as a wrapper around , leveraging the fact that polygon distance calculations can be efficiently approximated using bounding box distances. The function always sets the recheck flag to true, indicating that the exact distance must be recalculated at the heap tuple level for accurate results, as the bounding box approximation may not be precise enough.

## Parameters / Member Variables
- : Pointer to the GISTENTRY containing the polygon index entry (typically a bounding box representation)
- : The query datum (typically a point) to calculate distance from
- : The strategy number indicating the type of distance operation being performed
- : Object ID of the subtype (commented out and unused)
- : Boolean pointer set to indicate whether distance rechecking is required at the heap level

## Dependencies
- Functions called/Symbols referenced:
  - gist_bbox_distance
  - PG_GETARG_POINTER
  - PG_GETARG_DATUM
  - PG_GETARG_UINT16
  - PG_RETURN_FLOAT8
- Called from:
  - No direct references found (likely registered as operator class function)

## Notes and Other Information
- Always sets recheck=true because bounding box distance is an approximation for actual polygon distance
- Relies on gist_bbox_distance for the actual distance calculation logic
- Part of the GiST operator class infrastructure for geometric data types
- The distance returned is a float8 value representing the minimum distance between the query point and the polygon