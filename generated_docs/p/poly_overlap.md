# poly_overlap

## Location
src/backend/utils/adt/geo_ops.c: 3801 - 3829

## Overview
poly_overlap is a PostgreSQL geometric function that determines whether two polygons overlap or intersect.

## Definition
Datum poly_overlap(PG_FUNCTION_ARGS)

## Detailed Description
poly_overlap is a PostgreSQL function that implements the overlap operator for polygon data types. It serves as a wrapper function that extracts two POLYGON arguments from the function call arguments, delegates the actual overlap calculation to poly_overlap_internal(), and properly manages memory for potentially toasted input values. The function follows PostgreSQL's standard function calling convention using the PG_FUNCTION_ARGS macro and returns a boolean result indicating whether the two input polygons overlap.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS which provides access to:
  - First polygon (polya): The first POLYGON input argument
  - Second polygon (polyb): The second POLYGON input argument

## Dependencies
- Functions called/Symbols referenced:
  - [poly_overlap_internal](poly_overlap_internal.md): Core overlap detection algorithm
  - PG_GETARG_POLYGON_P: Extracts POLYGON argument from function arguments  
  - PG_FREE_IF_COPY: Manages memory for potentially toasted inputs
  - PG_RETURN_BOOL: Returns boolean result following PostgreSQL conventions
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:3801-3829
- Includes memory management for toasted inputs, which is essential for rtree indexes
- Acts as a thin wrapper around poly_overlap_internal() while handling PostgreSQL-specific argument extraction and memory management
- The function is designed to be called from SQL as the overlap operator (&) for polygon types