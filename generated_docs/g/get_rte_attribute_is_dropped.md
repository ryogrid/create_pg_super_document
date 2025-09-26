# get_rte_attribute_is_dropped

## Location
[src/backend/parser/parse_relation.c:3291-3438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3291-L3438)

## Overview
Checks whether an attempted attribute reference is to a dropped column within a range table entry (RTE).

## Definition

```c
structed,
				 * but one in a stored rule might contain columns that were
				 * dropped from the underlying tables, if said columns are
				 * nowhere explicitly referenced in the rule.  This will be
				 * signaled to us by a null pointer in the joinaliasvars list.
				 */
				Var		   *aliasvar;
```
## Detailed Description
This function determines if a specified attribute (column) in a range table entry has been dropped. It handles different types of RTEs with specific logic for each:

- **RTE_RELATION**: Queries the system catalog (pg_attribute) to check the  flag
- **RTE_SUBQUERY/RTE_TABLEFUNC/RTE_VALUES/RTE_CTE**: These never have dropped columns, so always returns false
- **RTE_NAMEDTUPLESTORE**: Checks for dropped columns by testing if the column type is valid
- **RTE_JOIN**: Checks if the joinaliasvars list contains a NULL pointer at the specified position, indicating a dropped column
- **RTE_FUNCTION**: For composite function results, checks the tuple descriptor to see if the column is dropped
- **RTE_RESULT**: Reports an error as this shouldn't normally happen

The function is essential for query planning and execution to avoid referencing columns that no longer exist in the underlying tables.

## Parameters / Member Variables
- : Range table entry to check for dropped attributes
- : Attribute number (column position) to check for dropped status

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md) (for catalog lookups)
  - [Int16GetDatum](../I/Int16GetDatum.md) (for datum conversion)
  - [list_nth_oid](../l/list_nth_oid.md) (for list operations)
  - [list_nth](../l/list_nth.md) (for list operations)
  - [get_expr_result_tupdesc](get_expr_result_tupdesc.md) (for function result type analysis)
  - Various RTE kind constants (RTE_RELATION, RTE_SUBQUERY, etc.)
- Called from (representative examples):
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md)
  - rt_fetch (via macro expansion)

## Notes and Other Information
- This function is crucial for maintaining data integrity when columns are dropped from tables
- The function handles stored rules that might reference dropped columns by checking for NULL pointers in join alias variable lists
- For function RTEs returning composite types, it performs deeper analysis of the result tuple descriptor
- The function includes comprehensive error handling for invalid attribute numbers and unrecognized RTE kinds