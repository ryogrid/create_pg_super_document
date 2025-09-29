# expect_integer_value

## Location
[src/common/compression.c:275-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/compression.c#L275-L310)

## Overview
A static utility function that parses and validates integer values for compression options within compression specification parsing.

## Definition

```c
static int
expect_integer_value(char *keyword, char *value, pg_compress_specification *result)
```
## Detailed Description
The  function is a helper function used during compression specification parsing to convert string values to integers. It validates that the provided value is not NULL and contains a valid integer representation. If parsing fails, it sets an appropriate error message in the result structure's parse_error field and returns -1. The function uses  for string-to-integer conversion and performs comprehensive validation to ensure the entire string represents a valid integer.

## Parameters / Member Variables
- : The name of the compression option being parsed (used for error reporting)
- : The string value to be parsed as an integer (may be NULL)
- : A pointer to the  structure where parse errors will be recorded

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL string formatting function)
  -  (standard C library function for string-to-long conversion)
  -  (structure type)
- Called from (representative examples):
  -  (src/common/compression.c:225) - for "level" option
  -  (src/common/compression.c:234) - for "workers" option

## Notes and Other Information
- Returns the parsed integer value on success, -1 on failure
- Validates that the value parameter is not NULL before attempting parsing
- Uses  with base 10 for decimal integer parsing
- Performs complete string validation by checking that the entire input string was consumed during parsing
- Sets detailed error messages in result->parse_error for different failure scenarios
- Error messages are internationalized using the  macro
- Function is declared static, limiting its scope to the compression.c source file
- Part of the internal implementation of the compression specification parsing system
- Used specifically for parsing integer-valued compression options like compression level and worker count

## Simplified Source

```c
static int
expect_integer_value(char *keyword, char *value, pg_compress_specification *result)
{
    int     ivalue;
    char   *ivalue_endp;

    if (value == NULL)
    {
        result->parse_error =
            psprintf(_("compression option \"%s\" requires a value"),
                     keyword);
        return -1;
    }

    ivalue = strtol(value, &ivalue_endp, 10);
    if (ivalue_endp == value || *ivalue_endp != '\0')
    {
        result->parse_error =
            psprintf(_("value for compression option \"%s\" must be an integer"),
                     keyword);
        return -1;
    }
    return ivalue;
}
```