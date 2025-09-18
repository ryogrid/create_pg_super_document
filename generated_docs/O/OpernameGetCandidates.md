# OpernameGetCandidates

## Location
src/backend/catalog/namespace.c: 1888 - 1927

## Overview
OpernameGetCandidates retrieves a list of all possible operator matches for a given possibly-qualified operator name and operator kind.

## Definition


## Detailed Description
This function performs comprehensive operator candidate lookup in the PostgreSQL catalog system. It returns all operators matching the specified name and kind, supporting both schema-qualified and unqualified searches. For schema-qualified names, it searches only within the specified schema. For unqualified names, it searches through all namespaces in the current search path. The function ensures that entries from earlier namespaces mask identical entries from later namespaces, preventing duplicates with identical argument lists. When oprkind is '\0', it returns all operators matching the name regardless of argument types.

## Parameters / Member Variables
- : List containing the operator name, possibly schema-qualified
- : Character indicating the operator kind ('l' for left unary, 'r' for right unary, 'b' for binary, '\0' for any)
- : Boolean flag indicating whether to return NULL or error when specified schema doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - DeconstructQualifiedName
  - LookupExplicitNamespace
  - recomputeNamespacePath
  - SearchSysCacheList1
  - CStringGetDatum
  - FuncCandidateList
  - CatCList
- Called from (representative examples):
  - oper
  - left_oper
  - regoperin
  - regoperout

## Notes and Other Information
- Returns FuncCandidateList with entries that always have two args[] slots (first is InvalidOid for prefix operators)
- The nargs field is always set to 2 regardless of actual operator arity
- Handles namespace masking to prevent duplicate candidates from multiple namespaces
- Used primarily in operator resolution and disambiguation during SQL parsing
- Critical for supporting PostgreSQL's operator overloading mechanism