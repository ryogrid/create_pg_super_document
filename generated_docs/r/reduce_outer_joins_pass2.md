# reduce_outer_joins_pass2

## Location
[src/backend/optimizer/prep/prepjointree.c:3084-3357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3084-L3357)

## Overview
Phase 2 processing function that examines qual clauses and performs actual outer join reductions based on strictness analysis and nullability constraints collected in pass 1.

## Definition
```c
static void reduce_outer_joins_pass2(Node *jtnode,
                                    reduce_outer_joins_pass1_state *state1,
                                    reduce_outer_joins_pass2_state *state2,
                                    PlannerInfo *root,
                                    Relids nonnullable_rels,
                                    List *forced_null_vars)
```

## Detailed Description
This function performs the core optimization logic of the outer join reduction algorithm. It recursively traverses the jointree and applies various transformations based on the analysis of quals and nullability constraints:

**Key Transformations Performed:**
1. **Outer-to-Inner Join Reduction**: Converts LEFT/RIGHT/FULL JOINs to INNER JOINs when upper quals force nullable-side relations to be non-null
2. **FULL Join Partial Reduction**: Reduces FULL JOINs to LEFT or RIGHT JOINs when only one side has nullability constraints
3. **JOIN_RIGHT Normalization**: Converts all RIGHT JOINs to LEFT JOINs by swapping arguments
4. **Anti-Semijoin Detection**: Converts LEFT JOINs to ANTI JOINs when join quals are strict for variables that are forced null by upper constraints

**Constraint Propagation Logic:**
- For INNER/SEMI joins: Merges and propagates both upper and local constraints to child nodes
- For LEFT/ANTI joins: Passes upper constraints to non-nullable side, local constraints to nullable side
- For FULL joins: No constraint propagation to avoid incorrect optimizations

The function maintains state about successfully reduced joins in state2, distinguishing between fully reduced joins (added to inner_reduced) and partially reduced FULL joins (tracked separately for custom nulling relation cleanup).

## Parameters / Member Variables
- `jtnode`: Current jointree node being processed (FromExpr or JoinExpr)
- `state1`: Pass 1 state data containing relation information and outer join indicators
- `state2`: Accumulator for information about successfully reduced joins
- `root`: Top-level planner state containing the parse tree
- `nonnullable_rels`: Set of base relation IDs that are forced non-null by upper quals
- `forced_null_vars`: Multibitmapset of variables that are forced null by upper quals

## Dependencies
- Functions called/Symbols referenced:
  - [find_nonnullable_rels](../f/find_nonnullable_rels.md)
  - [find_forced_null_vars](../f/find_forced_null_vars.md)
  - [find_nonnullable_vars](../f/find_nonnullable_vars.md)
  - [bms_add_members](../b/bms_add_members.md)
  - mbms_add_members
  - mbms_overlap_sets
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_free](../b/bms_free.md)
  - [report_reduced_full_join](report_reduced_full_join.md)
  - rt_fetch
  - linitial
  - lsecond
  - forboth
- Called from (representative examples):
  - [reduce_outer_joins](reduce_outer_joins.md) (initial call)
  - [reduce_outer_joins_pass2](reduce_outer_joins_pass2.md) (recursive calls)

## Notes and Other Information
- Static function internal to prepjointree.c, used only within the outer join reduction algorithm
- Requires that jtnode is never NULL or a base relation (RangeTblRef), as these should not appear in subtrees marked as contains_outer
- Updates both the JoinExpr node and corresponding RangeTblEntry when join types are changed
- Complex constraint propagation logic ensures that optimizations are applied safely without changing query semantics
- The function handles the intricate interactions between different join types and their nullability semantics
- Proper handling of SEMI/ANTI joins that may be introduced by sublink pullup