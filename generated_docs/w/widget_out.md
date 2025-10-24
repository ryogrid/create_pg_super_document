# widget_out

## Location
[src/test/regress/regress.c:205-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L205-L216)

## Overview
The widget_out function is an output function for the custom WIDGET data type, converting internal WIDGET structures into their string representations in PostgreSQL's regression testing framework.

## Definition
```c
Datum widget_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the output converter for the custom WIDGET data type, transforming the internal binary representation back into a human-readable string format. It takes a WIDGET structure (containing a center point and radius) and formats it as a string in the format "(x,y,radius)". The function uses PostgreSQL's psprintf function to create a formatted string that can be displayed to users or stored as text. This is the complementary function to widget_in, completing the input/output function pair required for custom PostgreSQL data types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - First argument: WIDGET pointer containing the widget data structure to be converted to string

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (macro for extracting pointer arguments)
  - [psprintf](../p/psprintf.md) (PostgreSQL's safe sprintf function for formatted string creation)
  - PG_RETURN_CSTRING (returns C-string value as Datum)
- Data types used:
  - [WIDGET](../W/WIDGET.md) (custom struct with Point center and double radius)
  - [Point](../P/Point.md) (geometric point structure with x and y coordinates)
  - Datum (PostgreSQL's generic data type)
  - char* (C-string for the output text)

- Called from (representative examples):
  - [WIDGET](../W/WIDGET.md) (referenced in the same file as part of type definition)

## Notes and Other Information
- This function is the output counterpart to widget_in, together forming the complete I/O function pair for the WIDGET data type
- Uses psprintf instead of sprintf for memory safety - [psprintf](../p/psprintf.md) automatically allocates sufficient memory
- Formats output as "(x,y,radius)" using %g format specifier for optimal double representation
- The %g format automatically chooses between fixed and exponential notation for optimal readability
- Part of PostgreSQL's regression testing framework demonstrating custom data type output functions
- Memory allocated by psprintf is automatically managed by PostgreSQL's memory context system
- Located in src/test/regress/regress.c as part of the test suite
- Demonstrates the standard pattern for implementing output functions in PostgreSQL user-defined types
- The WIDGET type was originally called "circle" but renamed to avoid conflicts with built-in types

## Simplified Source

```c
Datum
widget_out(PG_FUNCTION_ARGS)
{
    WIDGET *widget = (WIDGET *) PG_GETARG_POINTER(0);

    // Format widget as "(x,y,radius)" string
    char *str = psprintf("(%g,%g,%g)",
                        widget->center.x,
                        widget->center.y,
                        widget->radius);

    PG_RETURN_CSTRING(str);
}
```