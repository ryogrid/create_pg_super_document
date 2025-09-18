# create_upper_unique_plan

## Location
[src/backend/optimizer/plan/createplan.c:2281-2308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2281-L2308)

## Overview
Creates a Unique plan node for eliminating duplicate rows from upper-level query operations, using pathkey information to determine uniqueness criteria.

## Definition


## Detailed Description
The  function generates a Unique plan node from an UpperUniquePath, which is used to eliminate duplicate rows in upper-level query processing contexts (as opposed to base relation uniqueness). This function is part of PostgreSQL's plan creation infrastructure for handling DISTINCT operations and other uniqueness requirements.

Unlike some other plan node types, Unique nodes do not perform projection - they pass through their input unchanged except for eliminating duplicates. This means that target list requirements pass through to the subplan, but the function ensures that grouping columns are properly labeled using the CP_LABEL_TLIST flag.

The uniqueness determination is based on the pathkeys and the number of key columns specified in the UpperUniquePath, which defines which columns are used for duplicate detection.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information and context
- : UpperUniquePath structure representing the chosen uniqueness strategy and key specifications
- : Integer bitmask controlling plan creation behavior, automatically enhanced with CP_LABEL_TLIST

## Dependencies
- Functions called/Symbols referenced:
  - : Recursively creates execution plans for subpaths with enhanced flags
  - : Constructs Unique node from pathkey specifications and key count
  - : Copies common path information to the plan node
- Called from (representative examples):
  - : Main plan creation dispatch function

## Notes and Other Information
- This is a static function, only accessible within the createplan.c compilation unit
- Unique nodes are non-projecting, meaning they preserve the exact structure of their input tuples
- The CP_LABEL_TLIST flag is automatically added to ensure proper column labeling for uniqueness operations
- The  field from the path determines how many leading columns from the pathkeys are used for uniqueness
- This function handles upper-level uniqueness operations, distinct from base relation uniqueness constraints
- The pathkeys must already be established (typically through sorting) for the Unique node to function correctly
- Used in contexts like DISTINCT operations, UNION operations, and other scenarios requiring duplicate elimination