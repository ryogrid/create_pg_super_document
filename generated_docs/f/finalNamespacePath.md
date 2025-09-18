# finalNamespacePath

## Location
[src/backend/catalog/namespace.c:4198-4243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4198-L4243)

## Overview
Finalizes the namespace search path by removing duplicates, invoking namespace search hooks, and prepending implicitly-searched namespaces like pg_catalog and the temporary namespace.

## Definition
```c
static List *finalNamespacePath(List *oidlist, Oid *firstNS)
```

## Detailed Description
This function takes a list of namespace OIDs and produces the final search path by removing duplicates and applying namespace search hooks. It ensures that each namespace appears only once in the final list and invokes InvokeNamespaceSearchHook for each namespace to allow extensions to filter the search path. The function also automatically prepends implicitly-searched namespaces: pg_catalog (system catalog) is always included unless already present, and the session's temporary namespace is prepended if it exists and isn't already in the list. The order is important as namespaces are searched in the order they appear in the list.

## Parameters / Member Variables
- `oidlist`: Input list of namespace OIDs to process
- `firstNS`: Output parameter that receives the OID of the first explicitly-mentioned namespace

## Dependencies
- Functions called/Symbols referenced:
  - [list_member_oid](../l/list_member_oid.md)
  - InvokeNamespaceSearchHook
  - lappend_oid
  - linitial_oid
  - [lcons_oid](../l/lcons_oid.md)
  - [SearchPathCacheEntry](../S/SearchPathCacheEntry.md)
- Called from (representative examples):
  - [cachedNamespacePath](../c/cachedNamespacePath.md)

## Notes and Other Information
- Removes duplicate namespace entries from the input list
- Invokes namespace search hooks to allow extensions to filter namespaces
- Always prepends pg_catalog and temporary namespace (if valid) to the front of the list
- Does not check USAGE permissions for implicitly-searched namespaces
- Must be recalculated if object_access_hook is present due to potential hook result variations
- Returns a newly-allocated list that must be freed by the caller
- The firstNS parameter helps distinguish explicit from implicit mention of pg_catalog