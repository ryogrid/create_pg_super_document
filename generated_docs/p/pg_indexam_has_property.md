# pg_indexam_has_property

## Location
[src/backend/utils/adt/amutils.c:409-420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/amutils.c#L409-L420)

## Overview
SQL-callable function that tests whether a specific index access method supports a given property.

## Definition
```c
Datum pg_indexam_has_property(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL interface for querying access method-level properties. It serves as a thin wrapper around the indexam_property function, specifically configured for testing AM-wide capabilities. The function extracts the access method OID and property name from the SQL function arguments, then delegates to indexam_property with InvalidOid for the index parameter and 0 for the attribute number, indicating an AM-level property query. This allows SQL queries to determine capabilities like whether an access method can support unique indexes, multi-column indexes, or ordering constraints.

## Parameters / Member Variables
- Function takes SQL arguments via PG_FUNCTION_ARGS:
  - Argument 0: `amoid` (OID) - The object identifier of the index access method to test
  - Argument 1: `propname` (text) - The name of the property to check (e.g., 'can_unique', 'can_order')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (extract OID argument from SQL call)
  - PG_GETARG_TEXT_PP (extract text argument from SQL call)
  - [text_to_cstring](../t/text_to_cstring.md) (convert PostgreSQL text type to C string)
  - [indexam_property](../i/indexam_property.md) (core property testing logic)
- Called from (representative examples):
  - SQL queries testing AM capabilities
  - System catalog functions

## Notes and Other Information
- This is a PostgreSQL built-in function callable from SQL
- Specifically tests AM-level properties, not index-specific or column-specific properties
- Returns boolean values for known properties, NULL for unknown properties
- Common properties tested include: 'can_order', 'can_unique', 'can_multi_col', 'can_exclude', 'can_include'
- Used by applications and administrative tools to determine AM capabilities before creating indexes
- The function signature follows PostgreSQL's C function calling convention