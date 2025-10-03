# TSConfigIsVisible

## Location
[src/backend/catalog/namespace.c:3210-3221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3210-L3221)

## Overview
Determines whether a text search configuration (identified by OID) is visible in the current search path, meaning it would be found by searching for the unqualified text search configuration name.

## Definition

```c
bool
TSConfigIsVisible(Oid cfgid)
```
## Detailed Description
TSConfigIsVisible is a simple wrapper function that determines if a text search configuration is visible in the current namespace search path. It delegates the actual work to TSConfigIsVisibleExt with a NULL second parameter, which means any missing configuration will result in an error rather than setting a flag. The function checks whether the specified configuration would be found when searching by its unqualified name, taking into account the current search path order and potential name conflicts with configurations in other namespaces.

## Parameters / Member Variables
- `cfgid`: The OID (Object Identifier) of the text search configuration to check for visibility
## Dependencies
- Functions called/Symbols referenced:
  - [TSConfigIsVisibleExt](TSConfigIsVisibleExt.md)
- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md) (in objectaddress.c)
  - [regconfigout](../r/regconfigout.md) (in regproc.c)

## Notes and Other Information
- This is a convenience wrapper around TSConfigIsVisibleExt that uses the error-throwing behavior (NULL is_missing parameter)
- The function is part of PostgreSQL's namespace visibility system for text search configurations
- Visibility depends on the current search path and whether other configurations with the same name appear earlier in the path
- Located in src/backend/catalog/namespace.c:3210-3221

## Simplified Source

```c
bool
TSConfigIsVisible(Oid cfgid)
{
    // Simple wrapper that delegates to the extended version
    return TSConfigIsVisibleExt(cfgid, NULL);
}
```