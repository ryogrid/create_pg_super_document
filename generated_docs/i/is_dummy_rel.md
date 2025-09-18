# is_dummy_rel

## Location
[src/backend/optimizer/path/joinrels.c:1333-1381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1333-L1381)

## Overview
Determines whether a relation has been proven to be empty during query planning by examining its path structure for the characteristic childless Append pattern.

## Definition


## Detailed Description
This function checks if a RelOptInfo has been determined to contain no rows during the planning process. When the optimizer proves that a relation will be empty (typically through constraint exclusion or other logical analysis), it creates a special "dummy" path structure consisting of a childless Append node.

The function examines the relation's pathlist and looks for this characteristic dummy pattern. Initially, a dummy relation will have just one path - a childless Append with zero cost (which ensures it appears first in the pathlist). However, in later planning stages, additional projection layers might be added on top of the Append since Append nodes cannot perform projection themselves.

To handle these layered paths, the function uses a loop to descend through any ProjectionPath or ProjectSetPath wrappers until it reaches the underlying path. Once it finds the bottom-most path, it checks if it matches the IS_DUMMY_APPEND pattern.

This mechanism allows the optimizer to efficiently handle cases where entire relations or join results can be eliminated due to contradictory constraints, significantly improving query performance.

## Parameters / Member Variables
- : Pointer to the RelOptInfo being tested for emptiness

## Dependencies
- Functions called/Symbols referenced:
  -  - [Path](../P/Path.md) node type for projection operations
  -  - [Path](../P/Path.md) node type for set-returning function projections
  -  - Macro to test if an Append path is childless (dummy)

- Called from (representative examples):
  -  - Checks if relations are dummy before creating joins
  -  - Avoids path generation for dummy relations
  -  - Uses this to verify dummy status before marking
  -  - Macro that calls this function for external use

## Notes and Other Information
- Returns true if the relation is proven empty, false otherwise
- The function handles multiple layers of projection paths that may wrap the dummy Append
- A dummy relation will have zero-cost paths, ensuring they appear first in the pathlist
- This is part of PostgreSQL's constraint exclusion and logical optimization system
- The function is robust against various projection wrapper combinations
- Located in src/backend/optimizer/path/joinrels.c:1333-1381