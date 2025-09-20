# owningrel_does_not_exist_skipping

## Location
[src/backend/commands/dropcmds.c:139-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dropcmds.c#L139-L173)

## Overview
owningrel_does_not_exist_skipping is a helper function that determines whether a missing rule or trigger should be skipped because its owning relation or schema doesn't exist, rather than the object itself being missing.

## Definition

```c
static bool
owningrel_does_not_exist_skipping(List *object, const char **msg, char **name)
```
## Detailed Description
This function is used when a rule or trigger specification returns that the object doesn't exist. It checks whether the owning relation and its schema exist. If the owning relation or schema don't exist, it sets appropriate error message and name parameters and returns true (indicating the missing object should be skipped). If the owning relation exists, it returns false, meaning the rule/trigger itself is genuinely missing.

## Parameters / Member Variables
- : List representing the object specification (typically relation.schema.rule/trigger)
- : Output parameter for error message format string when skipping
- : Output parameter for the name to use in the error message

## Dependencies
- Functions called/Symbols referenced:
  - [list_copy_head](../l/list_copy_head.md): Creates a copy of the list excluding the last element (to get parent object)
  - [schema_does_not_exist_skipping](../s/schema_does_not_exist_skipping.md): Checks if the schema exists
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md): Converts name list to RangeVar
  - RangeVarGetRelid: Gets relation OID, returns InvalidOid if not found
  - [NameListToString](../N/NameListToString.md): Converts name list to string for error messages

- Called from (representative examples):
  - [does_not_exist_skipping](../d/does_not_exist_skipping.md): Used for rule and trigger object types

## Notes and Other Information
- This is a static function internal to dropcmds.c
- Specifically designed for rules and triggers which depend on owning relations
- Uses hierarchical checking: first checks schema, then relation existence
- Part of the missing_ok logic that allows graceful handling of non-existent objects
- Returns appropriate error messages for user feedback when objects are skipped