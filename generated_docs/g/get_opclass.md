# get_opclass

## Location
[src/backend/parser/parse_utilcmd.c:2026-2057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L2026-L2057)

## Overview
Fetches the qualified name of an index operator class, returning NIL if the operator class is the default for the given data type.

## Definition

```c
static List *
get_opclass(Oid opclass, Oid actual_datatype)
```
## Detailed Description
The  function retrieves the qualified name (schema and name) of an index operator class identified by its OID. The function performs an optimization by checking if the specified operator class is the default for the given data type - if it is, the function returns NIL to indicate that no explicit operator class specification is needed. This is used during index creation and constraint transformation to determine whether an operator class needs to be explicitly specified in the generated SQL.

The function uses the system catalog cache to look up the operator class information and compares it against the default operator class for the data type. When the operator class is not the default, it constructs a qualified name list containing both the schema name and operator class name.

## Parameters / Member Variables
- : The OID of the operator class to look up
- : The OID of the data type for which to check if this is the default operator class

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_opclass (system catalog structure)
  - [GetDefaultOpClass](../G/GetDefaultOpClass.md) (determines default operator class for a data type and access method)
  - [get_namespace_name](get_namespace_name.md) (retrieves schema name from namespace OID)
  - [makeString](../m/makeString.md) (creates a String node)
  - list_make2 (creates a two-element list)
- Called from (representative examples):
  - [generateClonedIndexStmt](generateClonedIndexStmt.md) (when cloning index statements)

## Notes and Other Information
- This is a static function in parse_utilcmd.c, used internally for parsing utility commands
- The function always schema-qualifies the name for simplicity when the operator class is not default
- Uses system catalog cache (CLAOID) for efficient operator class lookup
- Returns NIL when no explicit operator class specification is needed, which helps generate cleaner SQL
- Part of the index constraint transformation logic in PostgreSQL's parser