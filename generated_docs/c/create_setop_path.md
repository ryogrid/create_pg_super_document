# create_setop_path

## Location
src/backend/optimizer/util/pathnode.c: 3555 - 3616

## Overview
Creates a pathnode that represents computation of INTERSECT or EXCEPT set operations, supporting both sorted and hashed implementation strategies.

## Definition


## Detailed Description
This function creates a SetOpPath node that represents the execution of set operations like INTERSECT and EXCEPT (with or without ALL). The function supports two implementation strategies: sorted and hashed. In sorted mode, the input must already be sorted and the output preserves the sort order. In hashed mode, no particular input ordering is required. The cost calculation assumes one cpu_operator_cost per comparison per input tuple across all columns in the distinctList. SetOp operations don't project new columns, so they reuse the source path's pathtarget.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and configuration
- : RelOptInfo representing the parent relation associated with the result
- : Path representing the source of input data
- : SetOpCmd enum specifying the specific semantics (INTERSECT or EXCEPT, with/without ALL)
- : SetOpStrategy enum indicating implementation strategy (sorted or hashed)
- : List of SortGroupClause structures representing the grouping columns
- : AttrNumber indicating the column number where the flag column will be placed, if any
- : Integer flag value for the first input relation when using hashing strategy, or -1 when sorting
- : Double representing the estimated number of distinct groups
- : Double representing the estimated number of output rows

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - list_length
  - cpu_operator_cost
- Called from (representative examples):
  - [generate_nonunion_paths](../g/generate_nonunion_paths.md) (src/backend/optimizer/prep/prepunion.c:1179)

## Notes and Other Information
- SetOp operations don't project new columns, so they reuse the source path's pathtarget
- Assumes no parameterization (above any joins) for simplification
- Parallel safety depends on the relation's consider_parallel flag and subpath's parallel safety
- Preserves input sort order only when using SETOP_SORTED strategy
- Cost calculation charges one cpu_operator_cost per comparison per input tuple
- Assumes all columns in distinctList get compared for most tuples during execution
- The flagColIdx and firstFlag parameters are used for distinguishing between different input relations in the implementation
- Both INTERSECT and EXCEPT operations can be performed with or without the ALL keyword, affecting duplicate handling