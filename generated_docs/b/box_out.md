# box_out

## Location
src/backend/utils/adt/geo_ops.c: 455 - 465

## Overview
Converts the internal PostgreSQL BOX data type to its external string representation.

## Definition


## Detailed Description
The  function is a PostgreSQL output conversion function that transforms a BOX structure into its string representation. It utilizes the  function with  delimiter type to format the box as two coordinate pairs representing the corners of the rectangle. The function generates output in the format "(x1,y1),(x2,y2)" where the coordinates represent the high and low corners of the box. This is the inverse operation of  and is used whenever PostgreSQL needs to display or export box values as text.

## Parameters / Member Variables
- Uses  macro which provides:
  - : Pointer to a BOX structure containing the geometric data to be converted

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P
  - path_encode
  - PATH_NONE
  - PG_RETURN_CSTRING
  - BOX (struct type)
- Called from (representative examples):
  - This is a PostgreSQL output function, typically called by the PostgreSQL system when converting box values to string format for display or export

## Notes and Other Information
This function follows PostgreSQL's standard output function convention using the  interface. It leverages the existing  utility function by treating the box as a 2-point path with no surrounding delimiters (). The function assumes the BOX structure is already in normalized form (with proper high/low corner relationships) as maintained by  and other box manipulation functions. The resulting string format is compatible with the input format expected by , ensuring round-trip consistency.