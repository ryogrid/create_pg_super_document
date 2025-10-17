# text_format

## Location
[src/backend/utils/adt/varlena.c:5638-5914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5638-L5914)

## Overview
The  function implements PostgreSQL's  SQL function, providing sprintf-like string formatting with type-safe conversions and support for positional arguments.

## Definition

```c
struct_array(arr, element_type, elmlen, elmbyval, elmalign,
							  &elements, &nulls, &nitems);
```
## Detailed Description
This function provides a comprehensive string formatting facility similar to sprintf in C or printf-style functions. It processes a format string containing conversion specifiers and substitutes them with formatted representations of the provided arguments. Key features include:

- Support for variadic arguments (when called with VARIADIC keyword)
- Positional argument specification (e.g., %2$s)
- Width specification for padding/alignment
- Three conversion specifiers: 's' (string), 'I' (SQL identifier), 'L' (SQL literal)
- Proper handling of NULL values
- Type-safe argument conversion using PostgreSQL's type system

The function parses the format string character by character, handling literal text and conversion specifiers. For each conversion specifier, it:
1. Parses optional argument position and width specifications
2. Validates the conversion type ('s', 'I', or 'L')
3. Retrieves and converts the specified argument to string representation
4. Applies formatting flags and width specifications
5. Appends the formatted result to the output buffer

## Parameters / Member Variables
- Format string: The first argument containing the format template with conversion specifiers
- Variable arguments: The values to be formatted and substituted into the format string

## Dependencies
- Functions called/Symbols referenced:
  - , , , ,  - Argument handling
  - ,  - Function expression introspection
  - , , ,  - Array handling
  - ,  - [String](../S/String.md) buffer management
  -  - Parse format specifier components
  - , ,  - Type system integration
  - , ,  - Type conversions
  -  - Apply formatting to converted values
  - ,  - [Result](../R/Result.md) generation
  -  - Multibyte character handling
  - ,  - Memory management
- Called from (representative examples):
  -  - Non-variadic wrapper function
  - SQL FORMAT() function invocations

## Notes and Other Information
- Located in
- Supports both variadic and non-variadic calling conventions
- Implements comprehensive error checking for malformed format strings and insufficient arguments
- Optimizes type output function lookups by caching previous results
- Handles multibyte character encodings properly in error messages
- The format string syntax is: %[position$][flags][width]specifier
- Position specification allows reordering arguments (e.g., %2$s uses the 2nd argument)
- Width can be specified directly as a number or indirectly by referencing another argument
- Memory management follows PostgreSQL conventions with proper cleanup of temporary allocations

## Simplified Source

```c
Datum text_format(PG_FUNCTION_ARGS) {
    text *fmt;
    StringInfoData str;
    const char *cp;
    const char *start_ptr;
    const char *end_ptr;
    text *result;
    int arg;
    bool funcvariadic;
    int nargs;
    Datum *elements = NULL;
    bool *nulls = NULL;
    Oid element_type = InvalidOid;
    FmgrInfo typoutputfinfo;
    FmgrInfo typoutputinfo_width;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    // Handle variadic arguments by expanding array
    if (get_fn_expr_variadic(fcinfo->flinfo)) {
        ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
        element_type = ARR_ELEMTYPE(arr);
        // Extract array elements into elements[] and nulls[]
        deconstruct_array(arr, element_type, ...);
        funcvariadic = true;
    } else {
        nargs = PG_NARGS();
        funcvariadic = false;
    }

    // Initialize format string parsing
    fmt = PG_GETARG_TEXT_PP(0);
    start_ptr = VARDATA_ANY(fmt);
    end_ptr = start_ptr + VARSIZE_ANY_EXHDR(fmt);
    initStringInfo(&str);
    arg = 1;

    // Main loop: scan format string for conversion specifiers
    for (cp = start_ptr; cp < end_ptr; cp++) {
        // Copy literal characters
        if (*cp != '%') {
            appendStringInfoCharMacro(&str, *cp);
            continue;
        }

        // Handle %% -> single %
        if (*++cp == '%') {
            appendStringInfoCharMacro(&str, *cp);
            continue;
        }

        // Parse format specifier: %[position$][width]specifier
        int argpos, widthpos, flags, width;
        cp = text_format_parse_format(cp, end_ptr, &argpos, &widthpos, &flags, &width);

        // Validate specifier type (s, I, L)
        if (strchr("sIL", *cp) == NULL)
            ereport(ERROR, "unrecognized format specifier");

        // Get width value if indirect width specified
        if (widthpos >= 0) {
            Datum width_value = get_argument_value(widthpos or arg++);
            width = convert_to_int32(width_value);
        }

        // Get main argument value
        Datum value = get_argument_value(argpos or arg++);
        Oid typid = get_argument_type();

        // Convert value to string and apply formatting
        switch (*cp) {
            case 's':  // String conversion
            case 'I':  // SQL identifier
            case 'L':  // SQL literal
                text_format_string_conversion(&str, *cp, &typoutputfinfo,
                                            value, isNull, flags, width);
                break;
        }
    }

    // Generate final result
    result = cstring_to_text_with_len(str.data, str.len);
    PG_RETURN_TEXT_P(result);
}
```