# schema_does_not_exist_skipping

## Location
[src/backend/commands/dropcmds.c:174-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dropcmds.c#L174-L205)

## Overview
schema_does_not_exist_skipping is a helper function that determines whether a missing schema-qualifiable object should be skipped because its specified schema doesn't exist, rather than the object itself being missing.

## Definition

```c
static bool
schema_does_not_exist_skipping(List *object, const char **msg, char **name)
```
## Detailed Description
This function is used when a schema-qualified object specification returns that the object doesn't exist. It checks whether the specified schema exists. If no schema was specified or the schema exists, it returns false (meaning the object itself is missing). If the specified schema doesn't exist, it sets appropriate error message and name parameters and returns true (indicating the missing object should be skipped because its schema is missing).

## Parameters / Member Variables
- : List representing the schema-qualified object specification
- : Output parameter for error message format string when skipping
- : Output parameter for the schema name to use in the error message

## Dependencies
- Functions called/Symbols referenced:
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md): Converts name list to RangeVar to extract schema information
  - [LookupNamespaceNoError](../L/LookupNamespaceNoError.md): Looks up schema by name, returns InvalidOid if not found

- Called from (representative examples):
  - [owningrel_does_not_exist_skipping](../o/owningrel_does_not_exist_skipping.md): For checking parent relations
  - [type_in_list_does_not_exist_skipping](../t/type_in_list_does_not_exist_skipping.md): For checking type schemas
  - [does_not_exist_skipping](../d/does_not_exist_skipping.md): For various schema-qualified object types

## Notes and Other Information
- This is a static function internal to dropcmds.c
- Part of the hierarchical missing object checking system
- Used for schema-qualified objects like functions, types, operators, etc.
- Returns early if no schema is specified (rel->schemaname == NULL)
- Provides user-friendly error messages when schemas are missing
- Helps distinguish between missing schemas vs. missing objects within existing schemas