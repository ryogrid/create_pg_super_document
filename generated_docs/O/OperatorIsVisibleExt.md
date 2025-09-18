# OperatorIsVisibleExt

## Location
src/backend/catalog/namespace.c: 2061 - 2120

## Overview
OperatorIsVisibleExt determines whether an operator is visible in the current search path, with optional error handling for missing operators.

## Definition


## Detailed Description
This function provides extended operator visibility checking with enhanced error handling capabilities. It performs a comprehensive visibility test by first checking if the operator's namespace is in the current search path, then verifying that this specific operator would be found by name resolution (not masked by another operator with the same name and arguments earlier in the path). The function supports graceful handling of missing operators through the is_missing parameter, allowing callers to distinguish between invisible and non-existent operators.

## Parameters / Member Variables
- : OID of the operator to check for visibility
- : Optional pointer to boolean flag that will be set to true if the operator doesn't exist (caller must initialize to false)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - Form_pg_operator
  - recomputeNamespacePath
  - list_member_oid
  - OpernameGetOprid
  - makeString
  - ReleaseSysCache
- Called from (representative examples):
  - OperatorIsVisible
  - pg_operator_is_visible

## Notes and Other Information
- Performs a two-stage visibility check: namespace membership and name resolution precedence
- Uses OpernameGetOprid to ensure the operator isn't masked by another with higher precedence
- The is_missing parameter allows callers to handle non-existent operators without exceptions
- System catalog operators (PG_CATALOG_NAMESPACE) are always considered to be in the search path
- Critical for implementing PostgreSQL's operator resolution and visibility semantics