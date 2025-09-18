# MergeJoinClause

## Location
[src/include/nodes/execnodes.h:2134-2135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2134-L2135)

## Overview
MergeJoinClause is a typedef for a pointer to MergeJoinClauseData structure, representing runtime data for each individual mergejoin clause used in merge join operations.

## Definition


## Detailed Description
MergeJoinClause represents the runtime state for a single merge join clause in PostgreSQL's merge join algorithm. Each clause corresponds to an equality condition between expressions from the outer (left) and inner (right) relations. The structure maintains both the executable expression trees and cached evaluation results, along with comparison support data needed for the merge join algorithm. Multiple clauses can exist when joining on multiple columns, and they work together to establish the sort order for the merge operation.

## Parameters / Member Variables
- : ExprState pointer for the left-hand (outer relation) expression that will be evaluated
- : ExprState pointer for the right-hand (inner relation) expression that will be evaluated  
- : Cached Datum value from the most recent evaluation of the left expression
- : Cached Datum value from the most recent evaluation of the right expression
- : Boolean flag indicating whether the left expression result is NULL
- : Boolean flag indicating whether the right expression result is NULL
- : SortSupportData structure containing all comparison information needed to compare left and right values

## Dependencies
- Functions called/Symbols referenced:
  - [MergeJoinClauseData](MergeJoinClauseData.md) (actual structure being pointed to)
  - ExprState (for expression evaluation)
  - Datum (for cached values)
  - SortSupportData (for comparison support)
- Called from (representative examples):
  - [MJExamineQuals](MJExamineQuals.md) (examines join qualification clauses)
  - [MJEvalOuterValues](MJEvalOuterValues.md) (evaluates outer relation expressions)
  - [MJEvalInnerValues](MJEvalInnerValues.md) (evaluates inner relation expressions)
  - [MJCompare](MJCompare.md) (compares left and right values)
  - MarkInnerTuple (marks position for tuple restoration)

## Notes and Other Information
- This is a private structure defined in src/backend/executor/nodeMergejoin.c, not exposed in public headers
- The cached datum values avoid repeated expression evaluation when the same tuple is compared multiple times
- Multiple MergeJoinClause instances are used when joining on multiple columns, forming a composite sort key
- The SortSupportData enables efficient comparison operations optimized for the specific data types being compared
- The structure is part of the merge join algorithm which requires both input relations to be sorted on the join keys
- The typedef definition appears in src/include/nodes/execnodes.h at line 2134