# text_format_parse_digits

## Location
[src/backend/utils/adt/varlena.c:5915-5963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5915-L5963)

## Overview
The  function parses a sequence of consecutive decimal digits from a string pointer and converts them to an integer value with overflow checking.

## Definition

```c
static bool
text_format_parse_digits(const char **ptr, const char *end_ptr, int *value)
```
## Detailed Description
This static helper function is used by the PostgreSQL format string parser to extract numeric values from format specifiers. It implements safe integer parsing with the following characteristics:

- Parses only contiguous ASCII digits ('0' through '9')
- Uses overflow-safe arithmetic operations to prevent integer overflow
- Advances the parse pointer past all consumed digits
- Maintains parsing invariants required by the format string parser
- Returns whether any digits were successfully parsed

The function employs PostgreSQL's overflow-safe arithmetic macros ( and ) to detect integer overflow conditions and report appropriate errors. This ensures that maliciously crafted format strings cannot cause undefined behavior through integer overflow.

## Parameters / Member Variables
- `**ptr`: Pointer to the current parsing position (input/output parameter, advanced past parsed digits)
- `*end_ptr`: Pointer to the end of the string being parsed (boundary check)
- `*value`: Pointer to store the parsed integer value (output parameter)
## Dependencies
- Functions called/Symbols referenced:
  -  - 8-bit signed integer type definition
  -  - Overflow-safe 32-bit integer multiplication
  -  - Overflow-safe 32-bit integer addition
  -  - Macro to safely advance parsing pointer with bounds checking
  - , , ,  - PostgreSQL error reporting system
- Called from (representative examples):
  -  - Main format specifier parser (multiple call sites)
  -  - [Variable](../V/Variable.md)-length string datum extraction

## Notes and Other Information
- Located in
- Static function, only accessible within the same compilation unit
- Maintains parsing invariant that at least one character is available before string end
- Uses PostgreSQL's overflow-safe arithmetic to prevent security vulnerabilities
- Returns if any digits were parsed, if no digits were found at the current position
- On overflow, raises a error rather than returning invalid results
- The function modifies both the parse pointer and value through output parameters
- Part of the format string parsing infrastructure for the SQL function

## Simplified Source

```c
static bool text_format_parse_digits(const char **ptr, const char *end_ptr, int *value) {
    bool found = false;
    const char *cp = *ptr;
    int val = 0;

    // Parse contiguous digits
    while (*cp >= '0' && *cp <= '9') {
        int8 digit = (*cp - '0');

        // Check for overflow during val = val * 10 + digit
        if (pg_mul_s32_overflow(val, 10, &val) ||
            pg_add_s32_overflow(val, digit, &val)) {
            ereport(ERROR, "number is out of range");
        }

        ADVANCE_PARSE_POINTER(cp, end_ptr);
        found = true;
    }

    // Update output parameters
    *ptr = cp;
    *value = val;

    return found;  // true if any digits were parsed
}
```