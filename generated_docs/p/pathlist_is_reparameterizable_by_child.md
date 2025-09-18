# pathlist_is_reparameterizable_by_child

## Location
[src/backend/optimizer/util/pathnode.c:4571-4584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L4571-L4584)

## Overview
A helper function that determines whether all paths in a given path list can be reparameterized by a specific child relation during query optimization.

## Definition


## Detailed Description
This function iterates through a list of paths and checks whether each individual path can be reparameterizable by the specified child relation. Reparameterization is a query optimization technique where paths are adjusted to work with different parameter values, particularly in the context of partitioned tables or inheritance hierarchies. The function returns true only if all paths in the list can be successfully reparameterized; if any single path cannot be reparameterized, the entire function returns false.

The function is implemented as a simple loop that delegates the actual reparameterization check to the  function for each individual path. This provides a convenient way to validate entire path lists rather than checking paths individually.

## Parameters / Member Variables
- : A List containing Path structures that need to be checked for reparameterizability
- : A RelOptInfo pointer representing the child relation that the paths should be reparameterized for

## Dependencies
- Functions called/Symbols referenced:
  - path_is_reparameterizable_by_child (for individual path checking)
  - lfirst (list iteration macro)
  - foreach (list iteration macro)
- Called from (representative examples):
  - REJECT_IF_PATH_LIST_NOT_REPARAMETERIZABLE macro (line 4421 in pathnode.c)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pathnode.c compilation unit
- The function is primarily used through the REJECT_IF_PATH_LIST_NOT_REPARAMETERIZABLE macro, which provides early termination in optimization routines when reparameterization is not possible
- Returns false immediately upon finding the first non-reparameterizable path, making it an efficient short-circuit evaluation
- Located in src/backend/optimizer/util/pathnode.c:4571-4584