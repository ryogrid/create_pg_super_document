# uint64in_subr

## Location
[src/backend/utils/adt/numutils.c:987-1043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L987-L1043)

## Overview
Converts a string to an unsigned 64-bit integer using PostgreSQL's strtou64() function with error handling and optional partial parsing support.

## Definition

```c
uint64
uint64in_subr(const char *s, char **endloc,
			  const char *typname, Node *escontext)
```
## Detailed Description
This function provides string-to-unsigned-64-bit-integer conversion using PostgreSQL's custom strtou64() function for parsing. Similar to uint32in_subr, it offers flexible parsing with optional partial string processing and comprehensive error handling. The function is designed to handle the full range of 64-bit unsigned integers consistently across different platforms.

Unlike uint32in_subr, this function doesn't require additional platform compatibility checks since it uses PostgreSQL's own strtou64() implementation rather than the standard library's strtoul(). This ensures consistent behavior across all supported platforms and architectures.

## Parameters / Member Variables
- `*s`: Input string containing the integer representation to convert
- `**endloc`: Optional pointer to store the location where parsing stopped; if NULL, entire string must be valid
- `*typname`: Type name string used in error messages for better diagnostics
- `*escontext`: Error context node for soft error handling; if NULL, errors are thrown via ereport()
## Dependencies
- Functions called/Symbols referenced:
  - strtou64 (PostgreSQL's 64-bit unsigned integer parsing function)
  - ereturn (error return macro for soft error handling)
- Called from (representative examples):
  - [xid8in](../x/xid8in.md) (64-bit transaction ID input function)

## Notes and Other Information
- Uses PostgreSQL's custom strtou64() function rather than standard library functions
- Provides consistent 64-bit unsigned integer parsing across all platforms
- Supports partial string parsing when endloc parameter is provided
- Handles EINVAL and ERANGE errors from strtou64() appropriately
- Used for parsing PostgreSQL's 64-bit transaction ID (xid8) type
- Simpler implementation than uint32in_subr due to consistent strtou64() behavior
- Allows trailing whitespace when endloc is NULL
- No additional platform-specific range checking required

## Simplified Source

```c
uint64 uint64in_subr(const char *s, char **endloc,
                     const char *typname, Node *escontext) {
    uint64 result;
    char *endptr;

    // Parse string using PostgreSQL's strtou64 function
    errno = 0;
    result = strtou64(s, &endptr, 0);

    // Check for parsing errors
    if ((errno && errno != ERANGE) || endptr == s)
        ereturn(escontext, 0,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"",
                        typname, s)));

    // Check for numeric overflow
    if (errno == ERANGE)
        ereturn(escontext, 0,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s",
                        s, typname)));

    if (endloc) {
        // Caller wants to handle remaining string
        *endloc = endptr;
    } else {
        // Skip trailing whitespace and verify string is fully consumed
        while (*endptr && isspace(*endptr))
            endptr++;
        if (*endptr)
            ereturn(escontext, 0,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s: \"%s\"",
                            typname, s)));
    }

    return result;
}
```