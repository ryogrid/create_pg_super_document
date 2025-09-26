# pair_decode

## Location
src/backend/utils/adt/geo_ops.c: 212 - 254

## Overview  
A helper function that parses a coordinate pair (x,y) from a string representation, handling optional parentheses and proper delimiter validation for geometric data types.

## Definition
```c
static bool
pair_decode(char *str, float8 *x, float8 *y, char **endptr_p,
            const char *type_name, const char *orig_string,
            Node *escontext)
```

## Detailed Description
The `pair_decode` function parses a coordinate pair from a string, supporting both parenthesized "(x,y)" and non-parenthesized "x,y" formats. It handles whitespace, validates delimiters, and uses `single_decode` for parsing individual coordinate values. The function provides comprehensive error handling and can optionally report the stopping point in the string after successful parsing.

This function is fundamental to PostgreSQL's geometric data type parsing infrastructure, as coordinate pairs form the basic building blocks for points, line segments, paths, polygons, and other geometric objects.

## Parameters / Member Variables
- `str`: Input string containing the coordinate pair to be parsed
- `x`: Output parameter where the first coordinate (x-value) is stored  
- `y`: Output parameter where the second coordinate (y-value) is stored
- `endptr_p`: Optional pointer to a char pointer that will be set to the character following the parsed pair (NULL if full string parsing is required)
- `type_name`: Name of the geometric type being parsed (used for error messages)
- `orig_string`: Original input string (used for error context in messages)
- `escontext`: Error handling context for soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - `single_decode`: Used twice to parse the x and y coordinate values
  - `LDELIM`: Left delimiter constant (opening parenthesis)
  - `DELIM`: Coordinate separator delimiter (comma)
  - `RDELIM`: Right delimiter constant (closing parenthesis) 
  - `ereturn`: Error return macro for soft error handling
  - `isspace`: Standard library function for whitespace detection
- Called from (representative examples):
  - `point_in`: For parsing point input strings like "(1.0,2.0)"
  - `path_decode`: For parsing individual coordinate pairs within path strings
  - `circle_in`: For parsing circle center coordinates

## Notes and Other Information
- This is a static function, only accessible within the geo_ops.c file
- Returns `true` on successful parsing, `false` if an error occurred
- Supports both parenthesized and non-parenthesized coordinate pair formats
- Handles leading and trailing whitespace appropriately
- Uses PostgreSQL's soft error handling mechanism for graceful error recovery
- The function validates delimiter presence and proper sequence
- Part of PostgreSQL's comprehensive geometric data type system that maintains consistent parsing behavior across all geometric types
- Error messages include the original input string and type name for better user feedback