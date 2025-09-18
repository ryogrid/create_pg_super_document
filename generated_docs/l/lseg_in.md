# lseg_in

## Location
src/backend/utils/adt/geo_ops.c: 2065 - 2080

## Overview
PostgreSQL input function that parses string representations of line segments and converts them into internal LSEG data structures.

## Definition


## Detailed Description
The  function is a PostgreSQL type input function responsible for parsing various string formats representing 2D line segments and converting them into internal LSEG (line segment) data structures. This function is automatically called by PostgreSQL when converting string literals or text values to the lseg data type.

The function supports multiple input formats including:
-  - bracket notation
-  - parenthesis notation  
-  - comma-separated coordinates
-  - closed form (double parentheses)
-  - old form (single parentheses)

The function handles parsing errors gracefully and provides appropriate error context for debugging.

## Parameters / Member Variables
- Uses PostgreSQL's function argument system ()
  - Argument 0: Input string () obtained via 
  - Error context: Obtained from  for error reporting

## Dependencies
- Functions called/Symbols referenced:
  -  (line segment data type structure)
  -  (PostgreSQL memory allocation function)
  -  (function to parse geometric path/point strings)
  -  (macro to extract C string argument)
  -  (macro to return LSEG pointer result)  
  -  (macro to return NULL on parse failure)
- Called from (representative examples):
  - No direct references found (automatically invoked by PostgreSQL's type system)

## Notes and Other Information
- This is a PostgreSQL type input function with standard Datum signature
- Automatically invoked when PostgreSQL needs to convert text to lseg type
- Supports multiple string formats for maximum compatibility and usability
- Uses  with parameters:  for allowing open paths,  for exactly 2 points required
- Memory allocation via  ensures proper PostgreSQL memory context management  
- Returns NULL on parsing failure with appropriate error context for user feedback
- Part of PostgreSQL's geometric type system infrastructure
- The  parameter from  is used but not critically important for line segments (which are always "open" by definition)