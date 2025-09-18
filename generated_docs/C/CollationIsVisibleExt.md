# CollationIsVisibleExt

## Location
[src/backend/catalog/namespace.c:2419-2476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2419-L2476)

## Overview
The extended version of collation visibility checking that determines whether a collation is visible in the current search path, with optional error handling for missing collations.

## Definition
```c
static bool CollationIsVisibleExt(Oid collid, bool *is_missing)
```

## Detailed Description
This function performs comprehensive visibility checking for collations by first looking up the collation in the system catalog, then checking if it would be found through standard namespace resolution. It implements a two-stage visibility check: first verifying the collation's namespace is in the active search path, then performing a full resolution check to ensure the collation isn't shadowed by another collation of the same name earlier in the path.

The function includes graceful error handling - when is_missing is provided, it sets the flag instead of throwing an error for missing collations, making it suitable for contexts where missing objects should be handled gracefully.

## Parameters / Member Variables
- `collid`: The OID of the collation to check for visibility
- `is_missing`: Optional pointer to bool flag; if provided and collation is missing, sets *is_missing = true instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (to look up collation details)
  - Form_pg_collation (system catalog structure)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md) (to ensure search path is current)
  - [list_member_oid](../l/list_member_oid.md) (to check if namespace is in search path)
  - [CollationGetCollid](CollationGetCollid.md) (to perform full resolution check)
- Called from (representative examples):
  - [CollationIsVisible](CollationIsVisible.md) (the simple wrapper)
  - [pg_collation_is_visible](../p/pg_collation_is_visible.md) (SQL function interface)

## Notes and Other Information
- This is a static function, only visible within namespace.c
- Implements sophisticated visibility logic with namespace precedence handling
- Performs optimization by quickly rejecting collations not in search path before expensive resolution check
- System catalog collations (PG_CATALOG_NAMESPACE) are always considered to be in the path
- The final visibility check uses CollationGetCollid to ensure the same resolution logic is applied consistently
- Handles both error-throwing and graceful error handling modes via the is_missing parameter