# cannotCastJsonbValue

## Location
[src/backend/utils/adt/jsonb.c:2008-2037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L2008-L2037)

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

## Simplified Source

```c
static void cannotCastJsonbValue(enum jbvType type, const char *sqltype) {
    // Static lookup table for error messages
    static const struct {
        enum jbvType type;
        const char *msg;
    } messages[] = {
        {jbvNull, gettext_noop("cannot cast jsonb null to type %s")},
        {jbvString, gettext_noop("cannot cast jsonb string to type %s")},
        {jbvNumeric, gettext_noop("cannot cast jsonb numeric to type %s")},
        {jbvBool, gettext_noop("cannot cast jsonb boolean to type %s")},
        {jbvArray, gettext_noop("cannot cast jsonb array to type %s")},
        {jbvObject, gettext_noop("cannot cast jsonb object to type %s")},
        {jbvBinary, gettext_noop("cannot cast jsonb array or object to type %s")}
    };

    // Find matching type and report error
    for (int i = 0; i < lengthof(messages); i++) {
        if (messages[i].type == type) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg(messages[i].msg, sqltype)));
        }
    }

    // Fallback for unknown types (should be unreachable)
    elog(ERROR, "unknown jsonb type: %d", (int) type);
}
```