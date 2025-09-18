# scanNameSpaceForRelid

## Location
src/backend/parser/parse_relation.c: 240 - 281

## Overview
Searches the current parsing state's namespace for a relation item matching a specific relation OID, used for qualified name resolution.

## Definition


## Detailed Description
This static function searches through the p_namespace list to find a namespace item that corresponds to a specific relation OID. It is specifically designed for qualified name resolution where a schema.relation reference has been converted to a relation OID. The function only matches relation RTEs that have no alias (since qualified references cannot refer to aliased relations) and handles the same lateral scoping and ambiguity rules as its companion function scanNameSpaceForRefname.

## Parameters / Member Variables
- : Current parsing state containing the namespace list to search
- : The relation OID to search for
- : Source location in the query for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - check_lateral_ref_ok
  - ereport (for error reporting)
  - RTE_RELATION (enum constant)
- Called from (representative examples):
  - refnameNamespaceItem

## Notes and Other Information
- Only matches RTE_RELATION entries that have no alias (rte->alias == NULL)
- Implements the same visibility and lateral scoping rules as scanNameSpaceForRefname
- Reports ERRCODE_AMBIGUOUS_ALIAS when multiple relation items match the same OID
- Part of the qualified name resolution path in PostgreSQL's parser
- The comment "yes, the test for alias == NULL should be there..." indicates this behavior is intentional per SQL semantics