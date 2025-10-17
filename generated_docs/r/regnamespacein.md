# regnamespacein

## Location
[src/backend/utils/adt/regproc.c:1658-1699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1658-L1699)

## Overview
The regnamespacein function converts string representations of namespace (schema) names to their corresponding OID values for the regnamespace data type.

## Definition

```c
Datum
regnamespacein(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL input function that handles the conversion of text input to the regnamespace data type, which represents namespace (schema) references by their OID. The function accepts multiple input formats: namespace names as strings, numeric OIDs, or the special value "-" which signifies unknown (OID 0).

The function performs comprehensive input validation and error handling. It first attempts to parse the input as either a dash or numeric OID using parseDashOrOid. If that fails, it treats the input as a namespace name and performs a lookup in the pg_namespace system catalog. The function includes bootstrap mode checks and proper error reporting for invalid names or non-existent schemas.

The conversion process involves parsing qualified names, validating the syntax (single unqualified name expected), and performing a catalog lookup to find the corresponding namespace OID. The function supports PostgreSQL's error context mechanism for proper error reporting and handling.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function call interface (PG_FUNCTION_ARGS) which provides:
  - : Input string containing namespace name, OID, or "-"
  - : Error context for soft error handling
  - : Output OID value
  - : Parsed qualified name list

## Dependencies
- Functions called/Symbols referenced:
  - [parseDashOrOid](../p/parseDashOrOid.md) (parses "-" or numeric OID input)
  - PG_RETURN_OID (returns OID result)
  - IsBootstrapProcessingMode (checks if in bootstrap mode)
  - [stringToQualifiedNameList](../s/stringToQualifiedNameList.md) (parses qualified name syntax)
  - ereturn (soft error return mechanism)
  - [get_namespace_oid](../g/get_namespace_oid.md) (looks up namespace OID by name)
- Called from (representative examples):
  - [to_regnamespace](../t/to_regnamespace.md) (conversion function)

## Notes and Other Information
- Supports three input formats: namespace names, numeric OIDs, and "-" for unknown
- Includes proper bootstrap mode handling where only OID inputs are allowed
- Performs comprehensive error checking with appropriate error codes (ERRCODE_INVALID_NAME, ERRCODE_UNDEFINED_SCHEMA)
- Uses PostgreSQL's soft error handling mechanism through error context
- Part of the regnamespace type system for referencing database schemas
- Located in src/backend/utils/adt/regproc.c with other reg* type functions
- The function expects single unqualified namespace names, not dot-separated qualified names

## Simplified Source

```c
Datum regnamespacein(PG_FUNCTION_ARGS) {
    char *nsp_name_or_oid = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    Oid result;
    List *names;

    // Handle "-" (unknown namespace) or numeric OID input
    if (parseDashOrOid(nsp_name_or_oid, &result, escontext))
        PG_RETURN_OID(result);

    // Bootstrap mode only accepts OIDs
    if (IsBootstrapProcessingMode())
        elog(ERROR, "regnamespace values must be OIDs in bootstrap mode");

    // Parse namespace name and validate syntax
    names = stringToQualifiedNameList(nsp_name_or_oid, escontext);
    if (names == NIL)
        PG_RETURN_NULL();

    if (list_length(names) != 1)
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_INVALID_NAME),
                 errmsg("invalid name syntax")));

    // Look up namespace OID by name
    result = get_namespace_oid(strVal(linitial(names)), true);

    if (!OidIsValid(result))
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_UNDEFINED_SCHEMA),
                 errmsg("schema \"%s\" does not exist",
                        strVal(linitial(names)))));

    PG_RETURN_OID(result);
}
```