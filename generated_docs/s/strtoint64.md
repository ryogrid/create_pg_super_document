# strtoint64

## Location
[src/bin/pgbench/pgbench.c:988-1058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L988-L1058)

## Overview
A robust string-to-64-bit integer conversion function that provides comprehensive error handling and overflow detection, adapted from PostgreSQL's backend utilities.

## Definition
bool strtoint64(const char *str, bool errorOK, int64 *result)

## Detailed Description
strtoint64 is a specialized string parsing function that converts string representations of integers to 64-bit signed integers with comprehensive error handling. The function is based on pg_strtoint64() from PostgreSQL's backend but adapted for pgbench's specific needs. It implements a custom scanning algorithm rather than relying on sscanf to ensure consistent behavior across platforms. The function handles the full range of 64-bit integers including edge cases like INT64_MIN, uses overflow-safe arithmetic operations, and provides detailed error reporting. The implementation accumulates values as negative numbers to properly handle the asymmetric range of signed integers where the absolute value of the minimum exceeds the maximum positive value.

## Parameters / Member Variables
- str: Pointer to the null-terminated string to be converted to an integer
- errorOK: Boolean flag indicating whether to suppress error messages on conversion failure
- result: Pointer to int64 variable where the converted result will be stored on success

## Dependencies
- Functions called/Symbols referenced:
  - int8 (type for individual digit values)
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md) (overflow-safe 64-bit multiplication)
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md) (overflow-safe 64-bit subtraction)
  - PG_INT64_MIN (constant representing the minimum 64-bit signed integer value)
  - isspace (standard C library function for whitespace detection)
  - isdigit (standard C library function for digit detection)
  - pg_log_error (PostgreSQL logging function for error messages)

- Called from (representative examples):
  - [makeVariableValue](../m/makeVariableValue.md)
  - [PgBenchExprList](../P/PgBenchExprList.md)

## Notes and Other Information
- Provides more robust integer parsing than standard library functions like atoi or strtol
- Implements custom overflow detection using PostgreSQL's safe arithmetic functions
- Handles the asymmetric range of signed 64-bit integers correctly by accumulating as negative values
- Supports optional error message suppression for scenarios where silent failure is preferred
- Uses unlikely() hints for performance optimization on error paths
- Essential for safe parsing of numeric values in pgbench scripts and configuration
- Allows trailing whitespace but rejects other trailing characters for strict validation
- Returns conversion success status while storing the actual result via output parameter

## Simplified Source

```c
bool
strtoint64(const char *str, bool errorOK, int64 *result)
{
    const char *ptr = str;
    int64 tmp = 0;
    bool neg = false;

    // Skip leading whitespace
    while (*ptr && isspace((unsigned char) *ptr))
        ptr++;

    // Handle optional sign
    if (*ptr == '-') {
        ptr++;
        neg = true;
    } else if (*ptr == '+')
        ptr++;

    // Require at least one digit
    if (unlikely(!isdigit((unsigned char) *ptr)))
        goto invalid_syntax;

    // Process digits with overflow checking
    // Note: Accumulate as negative to handle INT64_MIN correctly
    while (*ptr && isdigit((unsigned char) *ptr)) {
        int8 digit = (*ptr++ - '0');

        if (unlikely(pg_mul_s64_overflow(tmp, 10, &tmp)) ||
            unlikely(pg_sub_s64_overflow(tmp, digit, &tmp)))
            goto out_of_range;
    }

    // Allow trailing whitespace only
    while (*ptr != '\0' && isspace((unsigned char) *ptr))
        ptr++;

    if (unlikely(*ptr != '\0'))
        goto invalid_syntax;

    // Convert result to positive if needed
    if (!neg) {
        if (unlikely(tmp == PG_INT64_MIN))
            goto out_of_range;
        tmp = -tmp;
    }

    *result = tmp;
    return true;

out_of_range:
    if (!errorOK)
        pg_log_error("value \"%s\" is out of range for type bigint", str);
    return false;

invalid_syntax:
    if (!errorOK)
        pg_log_error("invalid input syntax for type bigint: \"%s\"", str);
    return false;
}
```