# box_contain_pt

## Location
src/backend/utils/adt/geo_ops.c: 3146 - 3165

## Overview
This function tests whether a box contains a given point, serving as a PostgreSQL geometric operator for box-point containment relationships.

## Definition


## Detailed Description
The  function is a PostgreSQL geometric operator that determines if a box contains a specified point. It's implemented as a PostgreSQL function that takes two arguments (a box and a point) and returns a boolean result. The function acts as a wrapper around the  function, providing the geometric containment test with the box as the first argument and point as the second.

## Parameters / Member Variables
- : PostgreSQL function call context containing:
  - Argument 0:  - The box to test for containment
  - Argument 1:  - The point to test if contained within the box

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts box argument from function call
  -  - Extracts point argument from function call
  -  - Core function that performs the containment test
  -  - Returns boolean result to PostgreSQL
- Called from (representative examples):
  -  - SP-GiST quadtree inner node consistency check
  -  - SP-GiST quadtree leaf node consistency check

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operators
- Primarily used in SP-GiST (Space-Partitioned Generalized Search Tree) indexing for geometric data
- Located in src/backend/utils/adt/geo_ops.c:3146-3165
- The argument order (box first, point second) distinguishes this from  which takes point first
- Returns true if the point is inside the box or on its boundary