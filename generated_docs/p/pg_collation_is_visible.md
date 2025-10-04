# pg_collation_is_visible

## Location
[src/backend/catalog/namespace.c:4978-4991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4978-L4991)

## Overview
Determines whether a given collation is visible in the current search path, returning NULL if the collation does not exist.

## Definition
```c
Datum pg_collation_is_visible(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that checks the visibility of a collation identified by its OID in the current search path. It serves as a wrapper around the internal CollationIsVisibleExt function, providing a SQL interface for collation visibility checks. The function returns a boolean value indicating whether the collation is accessible from the current namespace context, or NULL if the collation doesn't exist in the system catalogs.

The visibility check considers both the current search path and database encoding compatibility. A collation is considered visible if it exists in a namespace that's in the current search path, wouldn't be shadowed by another collation with the same name in an earlier namespace, and is compatible with the current database encoding.

## Parameters / Member Variables
- First argument (OID): The object identifier of the collation to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extracts OID argument from function call
  - [CollationIsVisibleExt](../C/CollationIsVisibleExt.md): Internal function that performs the actual visibility check
  - PG_RETURN_NULL: Returns NULL value to SQL caller
  - PG_RETURN_BOOL: Returns boolean value to SQL caller
- Called from (representative examples):
  - Available as SQL function pg_collation_is_visible()

## Notes and Other Information
- This is a system information function available in SQL as pg_collation_is_visible(oid)
- Returns NULL rather than FALSE when the collation doesn't exist, following PostgreSQL's convention for visibility functions
- The function uses the is_missing parameter of CollationIsVisibleExt to distinguish between "not visible" and "doesn't exist"
- Part of PostgreSQL's namespace and visibility system for schema-qualified object resolution
- Unlike some other visibility functions, collation visibility also considers database encoding compatibility
- Located in src/backend/catalog/namespace.c:4978-4991

## Simplified Source

```c
Datum pg_collation_is_visible(PG_FUNCTION_ARGS) {
    Oid collation_oid = PG_GETARG_OID(0);
    bool is_missing = false;

    // Check if collation is visible in current search path
    bool result = CollationIsVisibleExt(collation_oid, &is_missing);

    // Return NULL if collation doesn't exist, otherwise return visibility status
    if (is_missing)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(result);
}
```