# box_out

## Location
[src/backend/utils/adt/geo_ops.c:455-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L455-L465)

## Overview
Converts the internal PostgreSQL BOX data type to its external string representation.

## Definition

```c
Datum
box_out(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL output conversion function that transforms a BOX structure into its string representation. It utilizes the  function with  delimiter type to format the box as two coordinate pairs representing the corners of the rectangle. The function generates output in the format "(x1,y1),(x2,y2)" where the coordinates represent the high and low corners of the box. This is the inverse operation of  and is used whenever PostgreSQL needs to display or export box values as text.

## Parameters / Member Variables
- Uses  macro which provides:
  - : Pointer to a BOX structure containing the geometric data to be converted

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P
  - [path_encode](../p/path_encode.md)
  - PATH_NONE
  - PG_RETURN_CSTRING
  - [BOX](../B/BOX.md) (struct type)
- Called from (representative examples):
  - This is a PostgreSQL output function, typically called by the PostgreSQL system when converting box values to string format for display or export

## Notes and Other Information
This function follows PostgreSQL's standard output function convention using the  interface. It leverages the existing  utility function by treating the box as a 2-point path with no surrounding delimiters (). The function assumes the BOX structure is already in normalized form (with proper high/low corner relationships) as maintained by  and other box manipulation functions. The resulting string format is compatible with the input format expected by , ensuring round-trip consistency.

## Simplified Source

```c
Datum box_out(PG_FUNCTION_ARGS) {
    BOX *box = PG_GETARG_BOX_P(0);

    // Convert box to string format using path_encode with no delimiters
    // Treats box as 2-point path: "(x1,y1),(x2,y2)"
    PG_RETURN_CSTRING(path_encode(PATH_NONE, 2, &(box->high)));
}
```