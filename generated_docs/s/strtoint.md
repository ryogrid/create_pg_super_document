# strtoint

## Location
[src/common/string.c:51-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/string.c#L51-L85)

## Overview
A wrapper function around the standard C library's  that converts strings to integer values, providing additional range checking to ensure the result fits within the  data type.

## Definition
```c
int strtoint(const char *pg_restrict str, char **pg_restrict endptr, int base)
```

## Detailed Description
This function serves as a safer alternative to directly using  when an  result is needed rather than a . It performs the same string-to-long conversion as , but adds an additional validation step to ensure the converted value fits within the range of an  data type. If the converted long value cannot be represented as an int, the function sets  to  to indicate a range error.

The function preserves all the behavioral characteristics of , including base conversion (binary, octal, decimal, hexadecimal) and pointer advancement for parsing multiple values from a string.

## Parameters / Member Variables
- `str`: The input string to convert to an integer, marked with  for optimization
- `endptr`: A pointer to a char pointer that will be set to point to the first character after the number in the string, marked with
- `base`: The number base for conversion (2-36, or 0 for automatic detection)
## Dependencies
- Functions called/Symbols referenced:
  - strtol (standard C library function)
- Called from (representative examples):
  - [DecodeDateTime](../D/DecodeDateTime.md) (src/backend/utils/adt/datetime.c:1034, 1188)
  - [DecodeTimeOnly](../D/DecodeTimeOnly.md) (src/backend/utils/adt/datetime.c:2017)
  - [DecodeTimeCommon](../D/DecodeTimeCommon.md) (src/backend/utils/adt/datetime.c:2606, 2637)
  - [nodeTokenType](../n/nodeTokenType.md) (src/backend/nodes/read.c:271)
  - [option_parse_int](../o/option_parse_int.md) (src/fe_utils/option_utils.c:58)

## Notes and Other Information
- The function is extensively used in date/time parsing routines throughout PostgreSQL
- Uses PostgreSQL's  keyword for performance optimization by indicating non-aliased pointers
- Essential for safe integer parsing where overflow detection is required
- Returns the converted integer value, but callers should check  to detect range errors
- The validation efficiently detects when a long value cannot be represented as an int

## Simplified Source

```c
int
strtoint(const char *pg_restrict str, char **pg_restrict endptr, int base)
{
    // Convert string to long using standard function
    long val = strtol(str, endptr, base);

    // Check if result fits in int range
    if (val != (int) val)
        errno = ERANGE;

    return (int) val;
}
```