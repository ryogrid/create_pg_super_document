# make_setop

## Location
src/backend/optimizer/plan/createplan.c: 6884 - 6939

## Overview
Creates a SetOp plan node that implements set operations like UNION, INTERSECT, and EXCEPT by filtering duplicate tuples based on specified columns.

## Definition


## Detailed Description
The  function constructs a SetOp plan node that performs set operations (UNION, INTERSECT, EXCEPT) on sorted input data. The node works by examining consecutive tuples in the sorted input stream and applying the specified set operation logic to eliminate duplicates or find intersections/differences. The function converts a list of SortGroupClause specifications into arrays of column indices, equality operators, and collations that the executor can use efficiently during runtime.

The SetOp node assumes its input is already sorted according to the distinctList specification. It processes tuples sequentially, comparing them using the provided equality operators and collations to determine whether to include, exclude, or mark tuples based on the set operation being performed.

## Parameters / Member Variables
- : The type of set operation to perform (UNION, INTERSECT, EXCEPT)
- : The execution strategy for the set operation (e.g., sorted vs hashed)
- : The input plan node providing sorted tuples to process
- : List of SortGroupClause objects identifying columns to compare for distinctness
- : Column index for a flag column used in some set operations
- : Value of the flag for the first input relation
- : Estimated number of distinct groups in the result

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates a new SetOp node)
  - list_length (gets the number of columns to compare)
  - palloc (allocates memory for operator arrays)
  - get_sortgroupclause_tle (extracts target entry from sort clause)
  - exprCollation (gets collation for expression comparison)
  - SetOpCmd, SetOpStrategy, SortGroupClause (related data types)
- Called from (representative examples):
  - create_setop_plan
  - CP_IGNORE_TLIST

## Notes and Other Information
- The function is static and only used within createplan.c
- Input data must be pre-sorted according to distinctList for correct operation
- Converts SortGroupClause list into parallel arrays (dupColIdx, dupOperators, dupCollations) for efficient executor access
- The flagColIdx and firstFlag parameters are used to distinguish between different input relations in complex set operations
- Memory allocation for operator arrays uses palloc, which is PostgreSQL's memory management system
- The numGroups parameter helps the executor estimate memory usage and choose appropriate algorithms