# enum_eq

## Location
[src/backend/utils/adt/enum.c:324-332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L324-L332)

## Overview
PostgreSQL built-in function that implements the equality comparison operator (=) for enum data types using direct OID comparison.

## Definition

```c
Datum
enum_eq(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the equality comparison functionality for PostgreSQL enum types. Unlike other enum comparison functions, enum_eq implements a highly optimized approach by performing direct OID equality comparison without consulting enum metadata or using enum_cmp_internal().

Since enum values are stored as unique OIDs within each enum type, two enum values are equal if and only if their OIDs are identical. This makes the equality check extremely fast and simple.

The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS and returns a boolean result wrapped in a Datum.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro to access function arguments:
  - First argument (index 0): Left-hand side enum OID
  - Second argument (index 1): Right-hand side enum OID

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (argument extraction macro)
  - PG_RETURN_BOOL (result return macro)
- Called from:
  - SQL queries using = operator with enum types
  - System catalog functions
  - [Query](../Q/Query.md) optimizer and executor

## Notes and Other Information
- Part of PostgreSQL's operator implementation framework for enum types
- Registered in the system catalogs as the implementation for the = operator on enum types
- Significantly more efficient than other comparison operators due to direct OID comparison
- Does not need to access type cache or enum metadata unlike other comparison functions
- Forms an optimized pair with enum_ne for equality/inequality testing
- Works correctly because each enum value has a unique OID within its type

## Simplified Source

```c
Datum
enum_eq(PG_FUNCTION_ARGS)
{
    Oid a = PG_GETARG_OID(0);  // First enum value OID
    Oid b = PG_GETARG_OID(1);  // Second enum value OID

    PG_RETURN_BOOL(a == b);    // Direct OID comparison
}
```

**Simplified Logic**: This function performs a simple equality test between two enum values by directly comparing their OID values. Since each enum value has a unique OID, equality is determined by OID equality.