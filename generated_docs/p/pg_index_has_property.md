# pg_index_has_property

## Location
[src/backend/utils/adt/amutils.c:421-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/amutils.c#L421-L432)

## Overview
SQL-callable function that tests whether a specific index supports a given property.

## Definition
```c
Datum pg_index_has_property(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL interface for querying index-level properties of a specific index instance. It serves as a thin wrapper around the indexam_property function, specifically configured for testing properties of individual indexes rather than access methods in general. The function extracts the index relation OID and property name from the SQL function arguments, then delegates to indexam_property with InvalidOid for the AM parameter and 0 for the attribute number, indicating an index-wide property query. This allows SQL queries to determine capabilities like whether a specific index supports clustering, different scan types, or backward scanning.

## Parameters / Member Variables
- Function takes SQL arguments via PG_FUNCTION_ARGS:
  - Argument 0: `relid` (OID) - The object identifier of the index relation to test
  - Argument 1: `propname` (text) - The name of the property to check (e.g., 'clusterable', 'index_scan', 'bitmap_scan')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (extract OID argument from SQL call)
  - PG_GETARG_TEXT_PP (extract text argument from SQL call)  
  - text_to_cstring (convert PostgreSQL text type to C string)
  - [indexam_property](../i/indexam_property.md) (core property testing logic)
- Called from (representative examples):
  - SQL queries testing index capabilities
  - System catalog functions
  - Administrative and monitoring tools

## Notes and Other Information
- This is a PostgreSQL built-in function callable from SQL
- Specifically tests index-level properties, not AM-wide or column-specific properties
- Returns boolean values for known properties, NULL for unknown properties
- Common properties tested include: 'clusterable', 'index_scan', 'bitmap_scan', 'backward_scan'
- Used by query planners, applications, and administrative tools to determine index capabilities
- The function automatically determines the access method from the provided index OID
- The function signature follows PostgreSQL's C function calling convention