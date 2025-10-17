# parseNumericOid

## Location
[src/backend/utils/adt/regproc.c:1843-1867](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1843-L1867)

## Overview
Determines if a C string represents a valid numeric OID and converts it to an OID value if so, providing safe parsing with error context support.

## Definition
```c
static bool parseNumericOid(char *string, Oid *result, Node *escontext)
```

## Detailed Description
The `parseNumericOid` function is a utility function that checks whether a given C string contains only numeric digits and, if so, converts it to a PostgreSQL OID (Object Identifier) value. This function is used as a fast path optimization in various registry type input functions to handle cases where users provide numeric OIDs directly instead of object names.

The function performs two main operations: first, it validates that the string contains only digits (0-9) using `strspn()` to ensure the entire string consists of numeric characters. If validation passes, it uses PostgreSQL's safe input function calling mechanism to convert the string to an OID via the `oidin()` function, with proper error context handling.

## Parameters / Member Variables
- `string`: The input C string to be tested and potentially converted
- `result`: Pointer to an Oid variable where the converted OID value will be stored
- `escontext`: Error context node for soft error handling; allows error reporting instead of throwing exceptions

## Dependencies
- Functions called/Symbols referenced:
  - [DirectInputFunctionCallSafe](../D/DirectInputFunctionCallSafe.md)
  - [oidin](../o/oidin.md)
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
- Called from (representative examples):
  - [regoperin](../r/regoperin.md)
  - [regoperatorin](../r/regoperatorin.md)
  - [parseDashOrOid](parseDashOrOid.md)

## Notes and Other Information
- Declared as static, indicating it's a private utility function within regproc.c
- Uses `strspn()` for efficient validation of numeric-only content
- Sets `*result` to `InvalidOid` when the string is not numeric to prevent uninitialized variable warnings
- The function safely ignores whether `oidin()` succeeds or fails when parsing, as the caller will handle validation
- Part of the broader registry type parsing infrastructure in PostgreSQL
- Serves as an optimization for direct OID input in contexts where both names and OIDs are acceptable
- Located in src/backend/utils/adt/regproc.c in the support routines section

## Simplified Source

```c
static bool parseNumericOid(char *string, Oid *result, Node *escontext) {
    // Check if string contains only digits
    if (string[0] >= '0' && string[0] <= '9' &&
        strspn(string, "0123456789") == strlen(string)) {

        Datum oid_datum;

        // Convert numeric string to OID using safe input function
        DirectInputFunctionCallSafe(oidin, string, InvalidOid, -1,
                                   escontext, &oid_datum);
        *result = DatumGetObjectId(oid_datum);
        return true;
    }

    // Not a numeric string - prevent uninitialized variable warnings
    *result = InvalidOid;
    return false;
}
```