# set_using_names

## Location
src/backend/utils/adt/ruleutils.c: 4145 - 4309

## Overview
Recursively assigns column aliases for merged USING columns during rule decompilation by traversing the query jointree and selecting appropriate unique column names.

## Definition


## Detailed Description
This function performs a recursive descent of the query jointree to select column aliases that will be used for merged USING columns in JOIN operations. It implements two strategies for name uniqueness depending on the dpns->unique_using flag:

1. **Global uniqueness strategy** (dpns->unique_using = true): Forces all USING names to be unique across the entire query level
2. **Local uniqueness strategy** (dpns->unique_using = false): Only requires USING names to be unique within their own join RTE

The function handles different types of jointree nodes:
- **RangeTblRef**: No action required
- **FromExpr**: Recursively processes all items in the fromlist
- **JoinExpr**: Main processing logic for join operations, including USING clause handling

For joins with USING clauses, the function selects appropriate column names by preferring user-written output aliases, honoring pushed-down names from parent joins, and ensuring uniqueness as required. Selected names are propagated down to child relations and stored for later use during rule decompilation.

## Parameters / Member Variables
- : Deparse namespace context containing column name tracking information and uniqueness strategy
- : Current jointree node being processed (RangeTblRef, FromExpr, or JoinExpr)
- : List of all USING aliases assigned in parent joins (must not be modified)

## Dependencies
- Functions called/Symbols referenced:
  - [identify_join_columns](../i/identify_join_columns.md)
  - rt_fetch
  - deparse_columns_fetch
  - [expand_colnames_array_to](../e/expand_colnames_array_to.md)
  - [make_colname_unique](../m/make_colname_unique.md)
  - [list_copy](../l/list_copy.md)
  - [list_nth](../l/list_nth.md)
  - nodeTag
- Called from (representative examples):
  - [set_deparse_for_query](set_deparse_for_query.md)
  - [set_using_names](set_using_names.md) (recursive calls)

## Notes and Other Information
- This function is part of PostgreSQL's rule decompilation system in ruleutils.c
- The function assumes that dpns->rtable_columns is already filled with pre-zeroed deparse_columns structs
- For unnamed joins, name requirements are pushed down to children rather than being resolved at the current level
- The function maintains parent-child relationships in the deparse_columns structures for proper name inheritance
- System columns (attribute numbers ≤ 0) are handled specially and not assigned names