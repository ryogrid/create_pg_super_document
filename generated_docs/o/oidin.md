# oidin

## Location
[src/backend/utils/adt/oid.c:37-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L37-L46)

## Overview
The oidin function converts a string representation of an object identifier (OID) into its internal binary format, serving as the input function for the OID data type in PostgreSQL.

## Definition

```c
Datum
oidin(PG_FUNCTION_ARGS)
```
## Detailed Description
The oidin function is responsible for parsing string input and converting it to PostgreSQL's internal OID representation. It uses the uint32in_subr helper function to perform the actual string-to-number conversion, ensuring proper error handling and validation. This function is part of PostgreSQL's type input/output system and is automatically called when converting text to OID values in SQL operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function call context and arguments
  -  (extracted via PG_GETARG_CSTRING(0)): Input string containing the OID value to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - [uint32in_subr](../u/uint32in_subr.md): Performs the actual string-to-uint32 conversion with error handling
  - PG_RETURN_OID: Macro to return the converted OID value
- Called from (representative examples):
  - [defGetObjectId](../d/defGetObjectId.md): Used in DDL command processing
  - [parseNumericOid](../p/parseNumericOid.md): Used in regproc type operations

## Notes and Other Information
- This function is registered as the input function for the OID data type in PostgreSQL's type system
- Error handling is delegated to uint32in_subr, which provides context-aware error messages
- The function follows PostgreSQL's V1 calling convention using the PG_FUNCTION_ARGS macro
- Location: src/backend/utils/adt/oid.c:37-46

## Simplified Source

```c
Datum oidin(PG_FUNCTION_ARGS) {
    char *s = PG_GETARG_CSTRING(0);
    Oid result;

    // Convert string to OID using uint32 parser
    result = uint32in_subr(s, NULL, "oid", fcinfo->context);

    PG_RETURN_OID(result);
}
```