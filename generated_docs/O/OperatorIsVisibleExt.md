# OperatorIsVisibleExt

## Location
[src/backend/catalog/namespace.c:2061-2120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2061-L2120)

## Overview
OperatorIsVisibleExt determines whether an operator is visible in the current search path, with optional error handling for missing operators.

## Definition

```c
static bool
OperatorIsVisibleExt(Oid oprid, bool *is_missing)
```
## Detailed Description
This function provides extended operator visibility checking with enhanced error handling capabilities. It performs a comprehensive visibility test by first checking if the operator's namespace is in the current search path, then verifying that this specific operator would be found by name resolution (not masked by another operator with the same name and arguments earlier in the path). The function supports graceful handling of missing operators through the is_missing parameter, allowing callers to distinguish between invisible and non-existent operators.

## Parameters / Member Variables
- : OID of the operator to check for visibility
- : Optional pointer to boolean flag that will be set to true if the operator doesn't exist (caller must initialize to false)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - Form_pg_operator
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [OpernameGetOprid](OpernameGetOprid.md)
  - [makeString](../m/makeString.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [OperatorIsVisible](OperatorIsVisible.md)
  - [pg_operator_is_visible](../p/pg_operator_is_visible.md)

## Notes and Other Information
- Performs a two-stage visibility check: namespace membership and name resolution precedence
- Uses OpernameGetOprid to ensure the operator isn't masked by another with higher precedence
- The is_missing parameter allows callers to handle non-existent operators without exceptions
- System catalog operators (PG_CATALOG_NAMESPACE) are always considered to be in the search path
- Critical for implementing PostgreSQL's operator resolution and visibility semantics