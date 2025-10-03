# RelationIsVisible

## Location
[src/backend/catalog/namespace.c:913-924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L913-L924)

## Overview
Determines whether a relation (identified by OID) is visible in the current search path, meaning it would be found by searching for the unqualified relation name.

## Definition

```c
bool
RelationIsVisible(Oid relid)
```
## Detailed Description
RelationIsVisible is a convenience wrapper function that checks if a relation is visible in the current namespace search path. It internally calls RelationIsVisibleExt with a NULL second parameter to perform the actual visibility check. A relation is considered "visible" if it would be found when searching for the unqualified relation name using the current search_path setting.

## Parameters / Member Variables
- `relid`: The OID of the relation to check for visibility
## Dependencies
- Functions called/Symbols referenced:
  - [RelationIsVisibleExt](RelationIsVisibleExt.md)
- Called from (representative examples):
  - [getRelationDescription](../g/getRelationDescription.md) (src/backend/catalog/objectaddress.c:4106)
  - [regclassout](../r/regclassout.md) (src/backend/utils/adt/regproc.c:976)
  - [generate_relation_name](../g/generate_relation_name.md) (src/backend/utils/adt/ruleutils.c:12862)
  - RangeVarGetRelid (src/include/catalog/namespace.h:94)

## Notes and Other Information
This function is a simple wrapper that provides backward compatibility and a simpler interface when the extended functionality of RelationIsVisibleExt is not needed. The function is defined in src/backend/catalog/namespace.c:913-924.

## Simplified Source

```c
bool RelationIsVisible(Oid relid) {
    // Simple wrapper that delegates to the extended version
    // with NULL for the second parameter (no error handling)
    return RelationIsVisibleExt(relid, NULL);
}
```