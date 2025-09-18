# make_pathkey_from_sortinfo

## Location
src/backend/optimizer/path/pathkeys.c: 197 - 254

## Overview
Creates a canonical PathKey from an expression and detailed sorting information, handling complex operator family relationships and equivalence class creation.

## Definition


## Detailed Description
This function is a comprehensive PathKey constructor that bridges the gap between raw sorting requirements and canonical PathKey representation. It performs several critical operations:

1. **Strategy Determination**: Maps reverse_sort parameter to appropriate B-tree strategy numbers (BTLessStrategyNumber or BTGreaterStrategyNumber).

2. **Operator Family Resolution**: Looks up the equality operator for the given operator family and discovers all related operator families through mergejoin relationships, as EquivalenceClasses need complete operator family information.

3. **EquivalenceClass Management**: Calls get_eclass_for_sort_expr to find or create an appropriate EquivalenceClass for the sort expression, handling complex scenarios involving child relations and sort references.

4. **Canonical PathKey Creation**: Finally delegates to make_canonical_pathkey to ensure the resulting PathKey is canonical and properly cached.

The function handles optional EquivalenceClass creation based on the create_it parameter, returning NULL if an EC doesn't exist and shouldn't be created.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context
- : The expression to be sorted on
- : Operator family OID defining sorting semantics
- : Input/comparison data type OID
- : Collation OID for text sorting
- : Boolean indicating descending sort order
- : Boolean indicating NULL value positioning
- : SortGroupRef from SortGroupClause, or zero if not applicable
- : Relids indicating specific relation context, or NULL
- : Boolean controlling EquivalenceClass creation

## Dependencies
- Functions called/Symbols referenced:
  - BTLessStrategyNumber, BTGreaterStrategyNumber (strategy constants)
  - [get_opfamily_member](../g/get_opfamily_member.md) (operator family lookup)
  - OidIsValid (OID validation)
  - elog (error logging)
  - [get_mergejoin_opfamilies](../g/get_mergejoin_opfamilies.md) (operator family discovery)
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md) (EquivalenceClass management)
  - [make_canonical_pathkey](make_canonical_pathkey.md) (canonical PathKey creation)
- Called from (representative examples):
  - [make_pathkey_from_sortop](make_pathkey_from_sortop.md)
  - [build_index_pathkeys](../b/build_index_pathkeys.md)
  - [build_partition_pathkeys](../b/build_partition_pathkeys.md)
  - [build_expression_pathkey](../b/build_expression_pathkey.md)

## Notes and Other Information
- Returns a canonical PathKey that might still be redundant with existing PathKeys
- Handles complex operator family relationships for mergejoinable operators
- Supports both creation and lookup modes via create_it parameter
- Critical for translating ORDER BY clauses and index ordering into PathKey representation
- Performs extensive error checking for missing operators and operator families
- Located in src/backend/optimizer/path/pathkeys.c:197-254