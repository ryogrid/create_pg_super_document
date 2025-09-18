# relation_has_unique_index_for

## Location
src/backend/optimizer/path/indxpath.c: 3440 - 3613

## Overview
Determines whether a relation provably has at most one row satisfying a set of equality conditions by checking if the conditions constrain all columns of some unique index.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's uniqueness analysis that determines if a given set of equality conditions can guarantee at most one matching row from a relation. It works by checking whether the conditions collectively constrain all columns of any unique index on the relation.

The function accepts conditions in two formats: RestrictInfo nodes (for join-derived conditions) and expression/operator pairs. It automatically incorporates usable baserestrictinfo clauses and performs comprehensive matching against all available unique indexes. For each unique index, it verifies that every key column is constrained by an appropriate equality condition with compatible operators from the index's opfamily.

## Parameters / Member Variables
- : PlannerInfo structure containing global query information
- : RelOptInfo structure representing the target relation
- : List of RestrictInfo nodes representing equality conditions (destructively modified)
- : List of expressions in the relation for equality matching
- : List of equality operators corresponding to exprlist expressions

## Dependencies
- Functions called/Symbols referenced:
  - list_length
  - bms_is_empty
  - lappend
  - [list_member_oid](../l/list_member_oid.md)
  - [get_rightop](../g/get_rightop.md)
  - [get_leftop](../g/get_leftop.md)
  - [match_index_to_operand](../m/match_index_to_operand.md)
  - forboth
  - lfirst_oid
  - [op_in_opfamily](../o/op_in_opfamily.md)
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure)
  - [RestrictInfo](../R/RestrictInfo.md) (structure)
- Called from (representative examples):
  - [rel_is_distinct_for](rel_is_distinct_for.md)
  - [create_unique_path](../c/create_unique_path.md)

## Notes and Other Information
- Automatically adds usable baserestrictinfo clauses to the analysis
- Only considers unique, immediately enforced, non-partial indexes
- Cannot use partial unique indexes even if predOK due to join predicate dependencies in check_index_predicates()
- Performs O(N^2) matching between conditions and index columns, assuming short lists
- Currently assumes all collations reduce to the same notion of equality (XXX comment indicates future enhancement needed)
- The restrictlist parameter is destructively modified during processing
- Returns true if any unique index has all its key columns constrained by the provided conditions
- File location: src/backend/optimizer/path/indxpath.c:3440-3613