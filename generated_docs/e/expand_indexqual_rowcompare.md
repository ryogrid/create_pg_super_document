# expand_indexqual_rowcompare

## Location
[src/backend/optimizer/path/indxpath.c:2798-3019](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L2798-L3019)

## Overview
Constructs detailed index scan conditions from RowCompareExpr clauses by analyzing additional columns beyond the first one and building optimized index qualifications.

## Definition
```c
static IndexClause *
expand_indexqual_rowcompare(PlannerInfo *root,
                           RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index,
                           Oid expr_op,
                           bool var_on_left)
```

## Detailed Description
This function represents the detailed implementation phase of row comparison optimization, called after match_rowcompare_to_indexcol() has determined that a RowCompareExpr can potentially use an index. It analyzes all columns in the row comparison to determine how many can be effectively used as index qualifications.

The function examines each column pair in the row comparison, checking if additional columns match index columns with compatible operators and strategies. When all columns match perfectly, it uses the original clause as-is. When only some columns match, it constructs a shortened RowCompareExpr or a simple OpExpr, potentially converting strict inequalities (< or >) to non-strict ones (<= or >=) to ensure all matching rows are found. This transformation makes the condition lossy but allows for more efficient index scans.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context
- `rinfo`: RestrictInfo containing the RowCompareExpr clause to be expanded
- `indexcol`: Starting column number within the index (first column that matched)
- `index`: IndexOptInfo structure with metadata about the target index
- `expr_op`: Operator OID for the first column comparison
- `var_on_left`: Boolean indicating whether indexed columns are on the left side of comparisons

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [get_op_opfamily_properties](../g/get_op_opfamily_properties.md)
  - list_make1_int
  - list_make1_oid
  - [list_nth](../l/list_nth.md)
  - [list_nth_oid](../l/list_nth_oid.md)
  - [get_commutator](../g/get_commutator.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [pull_varnos](../p/pull_varnos.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [match_index_to_operand](../m/match_index_to_operand.md)
  - [get_op_opfamily_strategy](../g/get_op_opfamily_strategy.md)
  - IndexCollMatchesExprColl
  - lappend_int
  - lappend_oid
  - [list_truncate](../l/list_truncate.md)
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [list_copy_head](../l/list_copy_head.md)
  - make_simple_restrictinfo
  - make_opclause
  - copyObject
- Called from (representative examples):
  - [match_rowcompare_to_indexcol](../m/match_rowcompare_to_indexcol.md)

## Notes and Other Information
- Performs detailed analysis of multi-column row comparisons for index optimization
- Tracks which specific index columns are used via the indexcols list
- Handles operator commutation when indexed columns are on the right side
- Converts strict inequalities to non-strict ones when building lossy conditions
- Creates shortened RowCompareExpr for partial matches or simple OpExpr for single column matches
- Sets the lossy flag when not all columns in the original comparison can be used
- Ensures all operators use the same strategy (all <, all <=, etc.) for consistency
- Critical for optimizing complex multi-column WHERE clauses and achieving efficient index scans
- Works exclusively with B-tree indexes due to their support for ordered comparisons