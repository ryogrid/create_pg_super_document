# SetOpPath

## Location
src/include/nodes/pathnodes.h: 2332 - 2342

## Overview
SetOpPath represents a path for set operations (INTERSECT, EXCEPT, UNION) in PostgreSQL's query planner, handling the combination and comparison of result sets from multiple queries.

## Definition


## Detailed Description
SetOpPath represents the execution path for set operations in PostgreSQL, specifically handling INTERSECT and EXCEPT operations (UNION is typically handled differently). This path type encapsulates the strategy for efficiently performing set operations between multiple input relations, including the method to use (hash-based or sort-based), the columns to compare for distinctness, and metadata for tracking which input relation contributed each row. The path handles both ALL and DISTINCT variants of set operations.

## Parameters / Member Variables
- : Base Path structure containing cost estimates, cardinality, and other standard path properties
- : Pointer to the input path that provides the combined source data from multiple relations
- : SetOpCmd enum value specifying the type of set operation (SETOPCMD_INTERSECT, SETOPCMD_EXCEPT)
- : SetOpStrategy enum indicating the execution method (SETOP_SORTED for sort-based, SETOP_HASHED for hash-based)
- : List of SortGroupClause structures identifying which columns are used for distinctness comparison
- : Column index of the flag column that identifies which input relation each row came from (0 if no flag column)
- : Integer value used as the flag for rows from the first input relation
- : Estimated cardinality representing the expected number of distinct groups in the input

## Dependencies
- Functions called/Symbols referenced:
  - SetOpCmd
  - SetOpStrategy
  - Cardinality
- Called from (representative examples):
  - create_setop_plan
  - create_setop_path
  - create_plan_recurse

## Notes and Other Information
- Set operations require careful handling of duplicate elimination and row source tracking
- The strategy choice (sorted vs hashed) depends on data characteristics and available memory
- Flag columns help track which input relation contributed each row during set operation processing
- The distinctList specifies exactly which columns are considered when determining row equality for set operations