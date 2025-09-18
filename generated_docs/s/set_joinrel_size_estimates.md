# set_joinrel_size_estimates

## Location
[src/backend/optimizer/path/costsize.c:5321-5352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5321-L5352)

## Overview
Sets the size estimates (row count) for a join relation by calculating the estimated number of rows the join will produce.

## Definition


## Detailed Description
This function is responsible for estimating the size (number of rows) of a join relation. It serves as a wrapper that calls  to perform the actual calculation and then assigns the result to the relation's rows field. The function assumes that the relation's targetlist has already been constructed and that an appropriate restriction clause list is provided.

The function acknowledges that for joins involving more than two base relations, the results could theoretically depend on which component relation pair is provided, though in practice the system aims for consistency. The estimation might vary slightly due to limitations in selectivity estimation routines, but this variation is considered acceptable rather than implementing complex averaging mechanisms.

Only the rows field is set by this function. The reltarget field is handled separately by , and  is not applicable to join relations.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query planning process
- : The join RelOptInfo whose size estimate is being set
- : The outer relation participating in the join
- : The inner relation participating in the join  
- : SpecialJoinInfo containing information about special join types (outer joins, semi-joins, etc.)
- : List of restriction clauses that apply to this join

## Dependencies
- Functions called/Symbols referenced:
  - [calc_joinrel_size_estimate](../c/calc_joinrel_size_estimate.md)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md) (struct type)
- Called from (representative examples):
  - build_join_rel (src/backend/optimizer/util/relnode.c:825)
  - build_child_join_rel (src/backend/optimizer/util/relnode.c:1000)

## Notes and Other Information
- This function is a thin wrapper around  and primarily serves to maintain a clean interface
- The function only sets the rows estimate; other relation properties are handled elsewhere
- Design acknowledges potential inconsistencies in multi-way joins but accepts them as a practical compromise
- Located in the cost estimation module of the PostgreSQL optimizer