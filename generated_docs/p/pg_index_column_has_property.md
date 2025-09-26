# pg_index_column_has_property

## Location
[src/backend/utils/adt/amutils.c:433-450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/amutils.c#L433-L450)

## Overview
This function tests a property of a specific column in an index, identified by the index OID and column number.

## Definition
```c
Datum pg_index_column_has_property(PG_FUNCTION_ARGS)
```

## Detailed Description
`pg_index_column_has_property` is a PostgreSQL system function that examines properties of individual columns within an index. It serves as a SQL-accessible interface to query various characteristics of index columns, such as ordering properties (ASC/DESC), null handling (NULLS_FIRST/NULLS_LAST), searchability, and returnability.

The function validates the column number (attno) to ensure it is positive (rejecting 0 or negative values) and then delegates the actual property testing to the `indexam_property` function. This design provides a clean separation between the SQL function interface and the core property testing logic.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (Oid relid): The OID of the index relation to examine
- `PG_FUNCTION_ARGS[1]` (int32 attno): The column number within the index (1-based)
- `PG_FUNCTION_ARGS[2]` (text propname): The name of the property to test (converted from text)

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - [indexam_property](../i/indexam_property.md)
- Called from (representative examples):
  - No direct callers found (likely called via SQL interface)

## Notes and Other Information
- Returns NULL for invalid column numbers (attno <= 0)
- Column numbers are 1-based, following PostgreSQL conventions
- The actual property testing logic is delegated to `indexam_property` with InvalidOid for amoid parameter
- This function is part of PostgreSQL's access method introspection system
- Available property names include: "asc", "desc", "nulls_first", "nulls_last", "orderable", "distance_orderable", "returnable", "search_array", "search_nulls"