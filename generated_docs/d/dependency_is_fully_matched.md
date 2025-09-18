# dependency_is_fully_matched

## Location
[src/backend/statistics/dependencies.c:595-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L595-L618)

## Overview
Checks whether a functional dependency is fully matched by verifying that all dependency attributes are covered by the provided set of attribute numbers.

## Definition


## Detailed Description
This function determines if a given functional dependency can be applied to optimize query planning by checking if all attributes involved in the dependency have corresponding equality clauses in the query. The function:

1. Iterates through all attributes in the functional dependency
2. For each attribute, checks if it exists in the provided bitmapset of available attributes
3. Returns true only if ALL dependency attributes are present in the bitmapset
4. Returns false if any dependency attribute is missing from the available set

This is a critical validation step used by the query planner to determine when functional dependencies can be applied for selectivity estimation and optimization purposes.

## Parameters / Member Variables
- : MVDependency structure containing the functional dependency to check
- : Bitmapset containing attribute numbers that have equality clauses available

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md) (bitmap set membership test function)
- Called from:
  - DependencyGenerator (during dependency enumeration)
  - [find_strongest_dependency](../f/find_strongest_dependency.md) (during query optimization)

## Notes and Other Information
- The function assumes that clauses on the attributes are suitable equality clauses
- Uses PostgreSQL's efficient bitmapset data structure for membership testing
- This is a static function, only visible within the dependencies.c compilation unit
- Essential for ensuring that functional dependencies are only applied when all required conditions are met
- Part of the query optimization logic that leverages extended statistics for better cardinality estimation
- Simple but critical validation function that prevents incorrect application of dependency statistics
- The bitmapset represents attributes that have usable equality predicates in the current query context