# Unique

## Location
src/include/nodes/plannodes.h: 1112 - 1127

## Overview
The Unique node is a plan node used to eliminate duplicate tuples from a sorted stream of data by comparing consecutive tuples and only returning the first tuple of each group of duplicates.

## Definition


## Detailed Description
The Unique node implements duplicate elimination by operating on top of a sorted input stream. It assumes that duplicate tuples arrive consecutively in the sorted order, allowing for efficient duplicate detection through simple comparison with the previously returned tuple. The node only returns the first tuple from each group of duplicates, effectively filtering out all subsequent identical tuples.

The node works by:
1. Fetching tuples from its child plan node (typically a Sort node)
2. Comparing each new tuple with the previously returned tuple using specified equality operators
3. If the tuples match on all specified columns, the new tuple is discarded
4. If the tuples differ, the new tuple is returned and becomes the new comparison baseline

## Parameters / Member Variables
- : Base Plan structure containing common plan node information
- : Number of columns to examine when determining tuple uniqueness
- : Array of column indexes in the target list to compare for uniqueness
- : Array of equality operator OIDs used for comparing corresponding columns
- : Array of collation OIDs for performing equality comparisons on each column

## Dependencies
- Functions called/Symbols referenced:
  - Plan (base structure)
  - AttrNumber
  - Oid
- Called from (representative examples):
  - ExecInitUnique
  - ExecUnique
  - create_upper_unique_plan
  - make_unique_from_sortclauses
  - make_unique_from_pathkeys

## Notes and Other Information
- The Unique node assumes its input is already sorted on the uniqueness columns
- It is typically placed above Sort nodes in the execution tree
- The node performs a streaming operation, processing one tuple at a time without storing the entire result set
- Equality comparisons use the specified operators and collations to handle different data types and locale-specific sorting rules
- The node is essential for implementing SQL DISTINCT operations and duplicate elimination in set operations