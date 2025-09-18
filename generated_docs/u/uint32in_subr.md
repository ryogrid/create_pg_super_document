# uint32in_subr

## Location
src/backend/utils/adt/numutils.c: 900 - 986

## Overview
Converts a string to an unsigned 32-bit integer using standard library functions with comprehensive error handling and optional partial parsing support.

## Definition


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