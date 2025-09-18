# dist_polyp

## Location
src/backend/utils/adt/geo_ops.c: 2621 - 2629

## Overview
A PostgreSQL function that calculates the distance between a polygon and a point, serving as a wrapper function for the internal distance calculation.

## Definition


## Detailed Description
This function implements the distance calculation between a POLYGON and a Point in PostgreSQL's geometric data types system. It serves as a SQL-callable wrapper that extracts the polygon and point arguments from the function call arguments and delegates the actual distance computation to the internal function . The function follows PostgreSQL's standard function calling convention using the  macro and returns a float8 datum representing the calculated distance.

## Parameters / Member Variables
- Function uses  macro which provides access to:
  - Argument 0:  - The polygon for distance calculation
  - Argument 1:  - The point for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts polygon argument
  -  - Extracts point argument  
  -  - Performs actual distance calculation
  -  - Returns float8 result
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This is a thin wrapper function that handles argument extraction and result formatting
- The actual distance computation logic is implemented in 
- Returns the shortest distance between any point on the polygon boundary/interior and the given point
- Part of PostgreSQL's geometric data types functionality in geo_ops.c