# match_rowcompare_to_indexcol

## Location
[src/backend/optimizer/path/indxpath.c:2691-2797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L2691-L2797)

## Overview
Analyzes RowCompareExpr clauses (multi-column comparisons) to determine if they can be converted into B-tree index scan conditions for query optimization.

## Definition
```c
static IndexClause *
match_rowcompare_to_indexcol(PlannerInfo *root,
                             RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index)
```

## Detailed Description
This function handles RowCompareExpr clauses, which represent multi-column comparisons like "(a,b) < (c,d)" or "(x,y,z) >= (1,2,3)". It focuses on determining whether the first column of the row comparison can be matched against the target index column, which is sufficient to establish that some useful index condition can be derived.

The function performs several key validations: it ensures the index is a B-tree (required for row comparisons), checks collation compatibility, verifies that one side contains the indexed column while the other side is a constant expression, and confirms that the comparison operator belongs to the appropriate strategy class for B-tree operations. When successful, it delegates the detailed index condition construction to expand_indexqual_rowcompare().

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and relation information
- `rinfo`: RestrictInfo containing the RowCompareExpr clause to be analyzed
- `indexcol`: Column number within the index being considered for optimization  
- `index`: IndexOptInfo structure with metadata about the candidate index

## Dependencies
- Functions called/Symbols referenced:
  - linitial
  - linitial_oid
  - IndexCollMatchesExprColl
  - [match_index_to_operand](match_index_to_operand.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [pull_varnos](../p/pull_varnos.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [get_commutator](../g/get_commutator.md)
  - [get_op_opfamily_strategy](../g/get_op_opfamily_strategy.md)
  - [expand_indexqual_rowcompare](../e/expand_indexqual_rowcompare.md)
- Called from (representative examples):
  - [match_clause_to_indexcol](match_clause_to_indexcol.md)

## Notes and Other Information
- Only works with B-tree indexes since they support ordered multi-column comparisons
- Checks the first column of the row comparison, leaving detailed processing to expand_indexqual_rowcompare()
- Handles both cases where the indexed column is on the left or right side of the comparison
- When the indexed column is on the right, it attempts to commute the operator to standardize the comparison
- Supports B-tree strategy operators: <, <=, >=, > for establishing scan bounds
- The function avoids matching based on opfamily alone to handle reverse-sort opfamilies correctly
- Essential for optimizing complex multi-column WHERE clauses and ORDER BY operations