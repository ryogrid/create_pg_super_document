# makeRangeVarFromNameList

## Location
[src/backend/catalog/namespace.c:3554-3593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3554-L3593)

## Overview
Converts a qualified name list (strings) into a RangeVar structure for relation identification and manipulation.

## Definition

```c
RangeVar *
makeRangeVarFromNameList(const List *names)
```
## Detailed Description
This utility function transforms a list of strings representing a qualified relation name into PostgreSQL's standard RangeVar structure. It handles different levels of qualification: simple relation names, schema-qualified names, and fully-qualified names with catalog specification.

The function supports up to three components in the name list: catalog name, schema name, and relation name. It uses a switch statement based on the list length to properly assign each component to the appropriate RangeVar field. The function enforces PostgreSQL's naming conventions by rejecting names with more than three components.

RangeVar is PostgreSQL's standard way of representing relation references in parsed SQL statements, making this function essential for converting textual relation references into the internal representation used throughout the system.

## Parameters / Member Variables
- `names`: A list of strings representing the qualified relation name components

## Dependencies
- Functions called/Symbols referenced:
  - [makeRangeVar](makeRangeVar.md) (to create the initial RangeVar structure)
  - list_length (to determine name components count)
  - strVal (to extract string values from list elements)
  - linitial/lsecond/lthird (to access list elements)
  - [NameListToString](../N/NameListToString.md) (for error message formatting)
  - ereport/ERROR (for error reporting)
- Called from (representative examples):
  - [get_relation_by_qualified_name](../g/get_relation_by_qualified_name.md)
  - [get_object_address_relobject](../g/get_object_address_relobject.md)
  - [RemoveRelations](../R/RemoveRelations.md)
  - [nextval](../n/nextval.md) (sequence operations)
  - [regclassin](../r/regclassin.md)
  - [pg_get_viewdef_name](../p/pg_get_viewdef_name.md)
  - Various utility and conversion functions

## Notes and Other Information
- Supports 1-3 name components: [relation], [schema, relation], [catalog, schema, relation]
- Throws ERRCODE_SYNTAX_ERROR for names with more than 3 components
- Creates RangeVar with location set to -1 (no specific source location)
- Essential for converting textual relation references to internal representation
- Used extensively in object address resolution and relation lookup operations
- Part of PostgreSQL's name resolution and qualification infrastructure
- Returns a newly allocated RangeVar structure that must be managed by the caller