# enum_first

## Location
[src/backend/utils/adt/enum.c:437-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L437-L465)

## Overview
A PostgreSQL built-in function that returns the first (minimum) value of an enum type.

## Definition
```c
Datum enum_first(PG_FUNCTION_ARGS)
```

## Detailed Description
The `enum_first` function implements the SQL function that returns the first value in the sort order of an enum type. It determines the enum type from the function call expression tree rather than examining the actual argument value, which means the argument can even be NULL. The function uses `enum_endpoint` with `ForwardScanDirection` to efficiently find the minimum enum value using the pg_enum system catalog's sort order index.

The function includes proper error handling for cases where the enum type cannot be determined or when the enum contains no values.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing metadata about the function call

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - [enum_endpoint](enum_endpoint.md)
  - ForwardScanDirection
  - ereport
  - [format_type_be](../f/format_type_be.md)
  - PG_RETURN_OID
- Called from:
  - SQL queries using enum_first() function

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL
- The actual argument value is not examined; the function derives the enum type from the expression tree
- Raises an error if the enum type cannot be determined from the calling context
- Raises an error if the enum type contains no values
- Returns the OID of the first enum value, which can be used in SQL operations

## Simplified Source

```c
Datum enum_first(PG_FUNCTION_ARGS) {
    // Get enum type from function call expression tree (not argument value)
    Oid enumtypoid = get_fn_expr_argtype(fcinfo->flinfo, 0);

    if (enumtypoid == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("could not determine actual enum type")));

    // Find the first enum value using forward scan
    Oid min = enum_endpoint(enumtypoid, ForwardScanDirection);

    if (!OidIsValid(min))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("enum %s contains no values",
                             format_type_be(enumtypoid))));

    PG_RETURN_OID(min);
}
```