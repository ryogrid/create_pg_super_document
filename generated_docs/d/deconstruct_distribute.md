# deconstruct_distribute

## Location
src/backend/optimizer/plan/initsplan.c: 1120 - 1271

## Overview
Processes qualification clauses from join tree nodes and distributes them to appropriate restriction and join lists during the second phase of join tree deconstruction.

## Definition


## Detailed Description
This function represents phase 2 of the join tree deconstruction process, responsible for taking qualification clauses extracted during the recursive scan and distributing them to their appropriate locations in the query plan structure. It handles different join tree node types with specialized processing:

**RangeTblRef nodes**: Processes any security barrier qualifications attached to the range table entry, ensuring proper handling of row-level security constraints.

**FromExpr nodes**: Handles both lateral-referencing qualifications that were postponed from child nodes and top-level WHERE clause qualifications, distributing them appropriately using distribute_quals_to_rels.

**JoinExpr nodes**: Performs the most complex processing:
- Creates SpecialJoinInfo structures for outer joins to track join semantics
- Handles postponed lateral clauses by incorporating them into join qualifications
- Implements special logic for LEFT JOINs with LHS-strict clauses, postponing non-degenerate clauses to enable join commutativity optimizations
- Manages ojscope calculation for proper qualification placement
- Adds SpecialJoinInfo entries to root->join_info_list for later use in join planning

The function ensures that qualification clauses are distributed to the correct RelOptInfo nodes while respecting outer join semantics and lateral reference constraints.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning state and target lists
- : The JoinTreeItem containing node information and collected qualification clauses

## Dependencies
- Functions called/Symbols referenced:
  - [process_security_barrier_quals](../p/process_security_barrier_quals.md)
  - [distribute_quals_to_rels](distribute_quals_to_rels.md)
  - [list_concat](../l/list_concat.md)
  - [make_outerjoininfo](../m/make_outerjoininfo.md)
  - [bms_union](../b/bms_union.md)
  - [bms_add_members](../b/bms_add_members.md)
  - nodeTag
- Called from (representative examples):
  - [deconstruct_jointree](deconstruct_jointree.md)

## Notes and Other Information
- Operates in the second phase after deconstruct_recurse has built the join tree structure
- Handles security barrier processing for row-level security enforcement
- Implements sophisticated postponement logic for LEFT JOIN clauses to enable join reordering optimizations
- The postponed_oj_qual_list mechanism allows handling of commutable left joins per algebraic identity 3
- Uses ojscope to control where outer join clauses can be placed relative to the join structure
- Creates SpecialJoinInfo nodes that guide later join planning decisions about valid join orders
- Distinction between degenerate and non-degenerate clauses affects postponement decisions
- Critical for ensuring SQL outer join semantics are preserved during optimization