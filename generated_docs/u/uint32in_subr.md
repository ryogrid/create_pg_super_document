# uint32in_subr

## Location
[src/backend/utils/adt/numutils.c:900-986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numutils.c#L900-L986)

## Overview
Converts a string to an unsigned 32-bit integer using standard library functions with comprehensive error handling and optional partial parsing support.

## Definition

```c
uint32
uint32in_subr(const char *s, char **endloc,
			  const char *typname, Node *escontext)
```
## Detailed Description
This function provides string-to-unsigned-32-bit-integer conversion using the standard library strtoul() function as its core parsing engine. It offers flexible parsing options including the ability to parse only a portion of the input string and return a pointer to the remaining unparsed content. The function handles cross-platform compatibility issues, particularly dealing with differences in unsigned long width across architectures.

The function includes special logic to handle cases where unsigned long is wider than uint32, ensuring consistent behavior across 32-bit and 64-bit platforms. It also provides backwards compatibility by accepting inputs with minus signs, validating the result through both signed and unsigned extension checks.

## Parameters / Member Variables
- : Input string containing the integer representation to convert
- : Optional pointer to store the location where parsing stopped; if NULL, entire string must be valid
- : Type name string used in error messages for better diagnostics
- : Error context node for soft error handling; if NULL, errors are thrown via ereport()

## Dependencies
- Functions called/Symbols referenced:
  - ereturn (error return macro for soft error handling)
  - PG_UINT32_MAX (maximum 32-bit unsigned integer constant)
- Called from (representative examples):
  - [oidin](../o/oidin.md) (object identifier input function)
  - [oidvectorin](../o/oidvectorin.md) (OID vector input function)
  - [oidparse](../o/oidparse.md) (OID parsing function)
  - [xidin](../x/xidin.md) (transaction ID input function)
  - [cidin](../c/cidin.md) (command ID input function)

## Notes and Other Information
- Uses standard library strtoul() for the actual parsing work
- Provides cross-platform compatibility for different unsigned long sizes
- Supports partial string parsing when endloc parameter is provided
- Handles both EINVAL and ERANGE errors from strtoul() appropriately
- Includes backwards compatibility for minus-sign prefixed inputs
- Used extensively for parsing PostgreSQL OID and transaction ID types
- Validates results on platforms where unsigned long exceeds uint32 range
- Allows trailing whitespace when endloc is NULL

## Simplified Source

```c
uint32
uint32in_subr(const char *s, char **endloc,
              const char *typname, Node *escontext)
{
    uint32 result;
    unsigned long cvt;
    char *endptr;

    // Parse string using standard library function
    errno = 0;
    cvt = strtoul(s, &endptr, 0);

    // Handle parsing errors (EINVAL treated same as no input parsed)
    if ((errno && errno != ERANGE) || endptr == s)
        ereturn(escontext, 0,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"",
                        typname, s)));

    // Handle range errors
    if (errno == ERANGE)
        ereturn(escontext, 0,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s",
                        s, typname)));

    // Handle end-of-string parsing
    if (endloc) {
        // Caller wants to parse rest of string separately
        *endloc = endptr;
    } else {
        // Skip trailing whitespace and ensure nothing else remains
        while (*endptr && isspace((unsigned char) *endptr))
            endptr++;
        if (*endptr)
            ereturn(escontext, 0,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s: \"%s\"",
                            typname, s)));
    }

    result = (uint32) cvt;

    // Cross-platform validation: handle cases where unsigned long > uint32
    // Accept inputs with minus signs for backwards compatibility
#if PG_UINT32_MAX != ULONG_MAX
    if (cvt != (unsigned long) result &&
        cvt != (unsigned long) ((int) result))
        ereturn(escontext, 0,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("value \"%s\" is out of range for type %s",
                        s, typname)));
#endif

    return result;
}
```