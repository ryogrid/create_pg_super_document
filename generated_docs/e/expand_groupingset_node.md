# expand_groupingset_node

## Location
[src/backend/parser/parse_agg.c:1657-1758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1657-L1758)

## Overview
A recursive function that expands GroupingSet nodes into their constituent grouping combinations, handling EMPTY, SIMPLE, ROLLUP, CUBE, and nested SET operations according to SQL GROUPING SETS semantics.

## Definition

```c
static List *
expand_groupingset_node(GroupingSet *gs)
```
## Detailed Description
This function is the core expansion engine for SQL GROUPING SETS, ROLLUP, and CUBE operations. It takes a single GroupingSet node and returns a list of lists, where each inner list represents one grouping combination:

1. **EMPTY Sets**: Returns a list containing one empty list, representing the grand total grouping.

2. **SIMPLE Sets**: Returns a list containing one list with the grouping expressions, representing a single grouping level.

3. **ROLLUP Sets**: Generates a hierarchical sequence of groupings by progressively including fewer expressions from left to right, plus an empty grouping. For ROLLUP(a,b,c), it generates: (a,b,c), (a,b), (a), ().

4. **CUBE Sets**: Generates all possible combinations of the grouping expressions using bit manipulation. For CUBE(a,b), it generates: (), (a), (b), (a,b).

5. **SET Sets**: Recursively expands nested GroupingSet nodes and concatenates their results, allowing complex combinations like ROLLUP within CUBE.

The function uses bit manipulation for efficient CUBE expansion and implements proper SQL standard semantics for each grouping set type.

## Parameters / Member Variables

Copyright (C) 2021 Artifex Software, Inc.  All rights reserved.
This software is supplied under the GNU AGPLv3 and comes with NO WARRANTY:
see the file COPYING for details.
GS>: The GroupingSet node to expand, containing:
  - : The type of grouping set (EMPTY, SIMPLE, ROLLUP, CUBE, or SETS)  
  - : The list of expressions or nested GroupingSet nodes to process

## Dependencies
- Functions called/Symbols referenced:
  - list_make1
  - list_length  
  - [list_concat](../l/list_concat.md)
  - lappend
  - lfirst
  - [expand_groupingset_node](expand_groupingset_node.md) (recursive call)
- Called from:
  - [expand_grouping_sets](expand_grouping_sets.md)
  - Self-recursion for nested SET processing

## Notes and Other Information
- This function implements the mathematical definitions from the SQL standard for GROUPING SETS operations
- CUBE expansion is limited to fewer than 31 elements to prevent exponential explosion (2^31 combinations)
- The bit manipulation technique for CUBE ensures all 2^n combinations are generated efficiently
- ROLLUP generates n+1 groupings for n input expressions (including the empty grouping)
- The function handles arbitrarily nested combinations through the SETS case and recursive calls
- Memory efficiency is maintained by reusing list operations rather than creating temporary structures