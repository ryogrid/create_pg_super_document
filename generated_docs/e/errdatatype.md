# errdatatype

## Location
[src/backend/utils/adt/domains.c:407-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/domains.c#L407-L430)

## Overview
A utility function that stores schema name and datatype name information of a specified datatype within the current error data context.

## Definition
```c
int errdatatype(Oid datatypeOid)
```

## Detailed Description
`errdatatype` is an error reporting helper function that enriches error messages with datatype context information. When called, it looks up the datatype information for the given OID and stores both the schema name and datatype name in the current error data structure using PostgreSQL's error reporting mechanism.

The function performs a system cache lookup to retrieve the pg_type tuple for the specified datatype OID, extracts the type name and namespace information, and then stores this information using the error reporting system's generic string fields. This enables error messages to include meaningful datatype context, making them more informative for users and applications.

The function is typically called as part of error reporting chains where datatype information is relevant to the error being reported.

## Parameters / Member Variables
- `datatypeOid`: The OID of the datatype whose schema and name information should be stored in the error context

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_type (system catalog form)
  - PG_DIAG_SCHEMA_NAME (error diagnostic field constant)
  - [err_generic_string](err_generic_string.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - PG_DIAG_DATATYPE_NAME (error diagnostic field constant)
- Called from (representative examples):
  - [ExecEvalConstraintNotNull](../E/ExecEvalConstraintNotNull.md)
  - [domain_check_input](../d/domain_check_input.md)
  - [errdomainconstraint](errdomainconstraint.md)

## Notes and Other Information
- The function always returns 0, as the return value is not meaningful
- Performs a system cache lookup that will throw an ERROR if the datatype OID is invalid
- Part of PostgreSQL's structured error reporting system
- Commonly used in constraint violation and type-related error reporting
- Stores information in standard PostgreSQL diagnostic fields that can be extracted by client applications
- Located at src/backend/utils/adt/domains.c:407-430

## Simplified Source

```c
int
errdatatype(Oid datatypeOid)
{
    HeapTuple tup;
    Form_pg_type typtup;

    // Look up the datatype in system cache
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(datatypeOid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", datatypeOid);

    typtup = (Form_pg_type) GETSTRUCT(tup);

    // Add schema and datatype name to error context
    err_generic_string(PG_DIAG_SCHEMA_NAME,
                       get_namespace_name(typtup->typnamespace));
    err_generic_string(PG_DIAG_DATATYPE_NAME, NameStr(typtup->typname));

    ReleaseSysCache(tup);

    return 0;  // Return value doesn't matter
}
```