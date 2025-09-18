# widget_in

## Location
src/test/regress/regress.c: 171 - 204

## Overview
The widget_in function is an input function for the custom WIDGET data type, converting string representations into internal WIDGET structures in PostgreSQL's regression testing framework.

## Definition
```c
Datum widget_in(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the input converter for the custom WIDGET data type, which represents a geometric object with a center point and radius (similar to a circle). It parses a string representation in the format "(x,y,radius)" and creates a WIDGET structure. The function expects exactly three numeric arguments separated by commas and enclosed in parentheses. If the input format is invalid or incomplete, it raises an ERROR rather than using soft error handling, which allows it to be used for testing hard-error scenarios in PostgreSQL.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - First argument: C-string (char*) containing the text representation of a widget in format "(x,y,radius)"

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting C-string arguments)
  - [palloc](../p/palloc.md) (PostgreSQL's memory allocation function)
  - atof (standard C function to convert string to double)
  - ereport (PostgreSQL's error reporting function)
  - [errcode](../e/errcode.md) (macro for error code specification)
  - [errmsg](../e/errmsg.md) (macro for error message formatting)
  - PG_RETURN_POINTER (returns pointer value as Datum)
- Constants used:
  - NARGS (defined as 3, number of expected arguments)
  - LDELIM (defined as '(', left delimiter)
  - RDELIM (defined as ')', right delimiter) 
  - DELIM (defined as ',', field delimiter)
- Data types used:
  - [WIDGET](../W/WIDGET.md) (custom struct with Point center and double radius)
  - [Point](../P/Point.md) (geometric point structure)
  - Datum (PostgreSQL's generic data type)

- Called from (representative examples):
  - [WIDGET](../W/WIDGET.md) (referenced in the same file as part of type definition)

## Notes and Other Information
- This function intentionally uses hard error handling (ereport with ERROR) rather than soft errors
- The WIDGET type was originally called "circle" but was renamed to avoid conflicts with PostgreSQL's built-in circle type
- Parses input format: "(x_coordinate,y_coordinate,radius_value)"
- Uses palloc for memory allocation, which is automatically freed by PostgreSQL's memory context system
- Part of PostgreSQL's regression testing framework to test custom data type functionality
- The parsing logic walks through the input string character by character to locate delimiters
- Located in src/test/regress/regress.c as part of the test suite
- Demonstrates how to implement input functions for user-defined data types in PostgreSQL