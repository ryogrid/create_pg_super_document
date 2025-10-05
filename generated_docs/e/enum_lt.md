# enum_lt

## Location
[src/backend/utils/adt/enum.c:306-314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L306-L314)

## Overview
PostgreSQL built-in function that implements the less-than comparison operator (<) for enum data types.

## Definition

```c
Datum
enum_lt(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the less-than comparison functionality for PostgreSQL enum types. It serves as a thin wrapper around enum_cmp_internal(), extracting the two enum OID arguments from the function call context and returning true if the first argument is less than the second according to the enum's defined ordering.

The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS and returns a boolean result wrapped in a Datum.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro to access function arguments:
  - First argument (index 0): Left-hand side enum OID
  - Second argument (index 1): Right-hand side enum OID

## Dependencies
- Functions called/Symbols referenced:
  - [enum_cmp_internal](enum_cmp_internal.md) (core comparison logic)
  - PG_GETARG_OID (argument extraction macro)
  - PG_RETURN_BOOL (result return macro)
- Called from:
  - SQL queries using < operator with enum types
  - System catalog functions
  - [Query](../Q/Query.md) optimizer and executor

## Notes and Other Information
- Part of PostgreSQL's operator implementation framework for enum types
- Registered in the system catalogs as the implementation for the < operator on enum types
- Performance relies on enum_cmp_internal's optimization strategies
- Returns true only if left operand has lower sort order than right operand in the enum definition

## Simplified Source

```c
Datum enum_lt(PG_FUNCTION_ARGS) {
    Oid left_enum = PG_GETARG_OID(0);
    Oid right_enum = PG_GETARG_OID(1);

    // Use internal comparison function and check if result is negative
    PG_RETURN_BOOL(enum_cmp_internal(left_enum, right_enum, fcinfo) < 0);
}
```