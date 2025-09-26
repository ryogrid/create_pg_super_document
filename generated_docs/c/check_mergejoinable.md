# check_mergejoinable

## Location
[src/backend/optimizer/plan/initsplan.c:3374-3410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L3374-L3410)

## Overview
Determines if a restriction clause is suitable for merge join operations and populates the mergejoin info fields in the RestrictInfo structure accordingly.

## Definition

```c
static void
check_mergejoinable(RestrictInfo *restrictinfo)
```
## Detailed Description
This function evaluates whether a given restriction clause can be used in merge join operations, which are one of PostgreSQL's fundamental join algorithms. Merge joins require the input relations to be sorted on the join keys and work by merging the sorted streams together, making them particularly efficient for large datasets that are already sorted or can be efficiently sorted.

The function performs several validation steps:
1. Skips pseudoconstant clauses (which don't involve variables from multiple relations)
2. Verifies the clause is an operator expression (OpExpr)
3. Ensures the operator is binary (has exactly two arguments)
4. Checks if the operator is mergejoinable using the system catalogs
5. Verifies that no volatile functions are present in the restriction (volatile functions would make merge join unsafe due to their unpredictable behavior)

If all conditions are met, the function populates the mergeopfamilies field of the RestrictInfo with the appropriate operator families, enabling the query planner to consider merge join strategies for this clause.

## Parameters / Member Variables
- : RestrictInfo structure containing the clause to be evaluated and metadata fields to be populated if the clause is mergejoinable

## Dependencies
- Functions called/Symbols referenced:
  - [is_opclause](../i/is_opclause.md) (checks if expression is an operator clause)
  - [op_mergejoinable](../o/op_mergejoinable.md) (determines if operator supports merge joins)
  - [contain_volatile_functions](contain_volatile_functions.md) (checks for volatile function calls)
  - [get_mergejoin_opfamilies](../g/get_mergejoin_opfamilies.md) (retrieves relevant operator families)
  - [exprType](../e/exprType.md) (determines expression data type)
  - linitial (gets first list element)
  - [OpExpr](../O/OpExpr.md) (operator expression node type)
- Called from:
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md) (during initial clause distribution)
  - [process_implied_equality](../p/process_implied_equality.md) (when processing equivalence class implications)
  - [build_implied_join_equality](../b/build_implied_join_equality.md) (when building implied equality conditions)

## Notes and Other Information
- This is a static function within initsplan.c, indicating it's an internal utility for query planning
- The op_mergejoinable check is described as "just a hint" - the definitive test is whether operator families are found
- If no btree operator families are found for the operator, the clause will not be treated as mergejoinable regardless of the op_mergejoinable result
- Merge joins require ordered data, so this function is closely tied to PostgreSQL's btree indexing infrastructure
- The exclusion of volatile functions is critical for correctness, as merge joins may not evaluate all combinations of rows
- The mergeopfamilies field populated by this function is used later in the planning process to determine join strategies and costs