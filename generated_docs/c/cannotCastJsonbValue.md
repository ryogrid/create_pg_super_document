# cannotCastJsonbValue

## Location
src/backend/utils/adt/jsonb.c: 2008 - 2037

## Overview
Static helper function that generates appropriate, translatable error messages when JSONB values cannot be cast to specific SQL types.

## Definition
```c
static void cannotCastJsonbValue(enum jbvType type, const char *sqltype)
```

## Detailed Description
This function provides centralized error reporting for JSONB type casting failures in PostgreSQL. It maintains a static lookup table that maps each JSONB value type to an appropriate error message template. When a casting operation fails, this function looks up the appropriate error message for the given JSONB type and reports it using PostgreSQL's error reporting system with proper internationalization support.

The function covers all possible JSONB value types including null, string, numeric, boolean, array, object, and binary. Each type has its own specific error message template that includes the target SQL type in the error message. The function uses gettext_noop for internationalization support, allowing the error messages to be translated into different languages.

## Parameters / Member Variables
- `type`: enum jbvType representing the JSONB value type that failed to cast
- `sqltype`: const char* representing the target SQL type name for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - gettext_noop (for internationalization)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md) (for error code specification)
  - [errmsg](../e/errmsg.md) (for error message formatting)
  - elog (for fallback error reporting)
  - lengthof (for array length calculation)
- Types/Constants referenced:
  - jbvType enum and its values:
    - jbvNull, jbvString, jbvNumeric, jbvBool, jbvArray, jbvObject, jbvBinary
  - ERRCODE_INVALID_PARAMETER_VALUE
- Called from (representative examples):
  - [jsonb_bool](../j/jsonb_bool.md)
  - [jsonb_numeric](../j/jsonb_numeric.md)
  - [jsonb_int2](../j/jsonb_int2.md), jsonb_int4, jsonb_int8
  - [jsonb_float4](../j/jsonb_float4.md), jsonb_float8

## Notes and Other Information
- Static function, only accessible within the same source file
- Provides consistent error messaging across all JSONB casting functions
- Supports internationalization through gettext_noop
- Contains a fallback error for unknown JSONB types (should be unreachable in normal operation)
- Uses PostgreSQL's standard error reporting mechanism with appropriate error codes
- Located in src/backend/utils/adt/jsonb.c:2008-2037
- Essential for providing user-friendly error messages during type conversion failures