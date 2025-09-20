# pair_count

## Location
[src/backend/utils/adt/geo_ops.c:392-421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L392-L421)

## Overview
Counts the number of coordinate pairs in a delimited string representation of geometric data, supporting both nested and flat coordinate notation.

## Definition

```c
static int
pair_count(char *s, char delim)
```
## Detailed Description
The  function analyzes a string containing geometric coordinate data and determines how many coordinate pairs it represents. It supports two different coordinate notation formats: nested format like '((1,2),(3,4))' and flat format like '(1,3,2,4)'. The function counts delimiter characters and uses the mathematical property that valid coordinate pairs require an odd number of delimiters. If an even number of delimiters is found, the function returns -1 to indicate invalid input.

## Parameters / Member Variables
- : Pointer to a null-terminated string containing the coordinate data to be analyzed
- : Character used as delimiter (typically comma ',') to separate coordinate values

## Dependencies
- Functions called/Symbols referenced:
  - strchr (standard C library function)
- Called from (representative examples):
  - [path_in](path_in.md)
  - [poly_in](poly_in.md)

## Notes and Other Information
This function implements a validation mechanism for geometric input parsing. The requirement for an odd number of delimiters stems from the coordinate pair structure: each pair needs an internal delimiter (x,y) plus separators between pairs. The function returns -1 for invalid input, which allows calling functions to detect malformed geometric data early in the parsing process. This is a static utility function used internally by PostgreSQL's geometric data type input functions.