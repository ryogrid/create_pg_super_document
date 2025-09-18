# QTW_EXAMINE_RTES_AFTER

## Location
src/include/nodes/nodeFuncs.h: 29 - 30

## Overview
A flag bit constant used to control the behavior of query_tree_walker and query_tree_mutator functions, specifically instructing them to examine Range Table Entry (RTE) nodes after processing their contents.

## Definition


## Detailed Description
QTW_EXAMINE_RTES_AFTER is a bit flag with value 0x20 (32 in decimal) that controls the traversal order when walking through PostgreSQL's query tree structures. When this flag is set, the walker function will examine Range Table Entry nodes after descending into and processing their contents first. This is the counterpart to QTW_EXAMINE_RTES_BEFORE and is useful for operations that need to process the expressions and subqueries within an RTE before examining the RTE structure itself.

This post-order traversal approach is particularly valuable for operations like optimization passes that need to understand the fully processed contents of an RTE before making decisions about the RTE itself, or for cleanup operations that need to process nested structures before their containers.

## Parameters / Member Variables
- Value:  (hexadecimal) - The bit flag value used in bitwise operations with other QTW flags

## Dependencies
- Functions called/Symbols referenced:
  - (This is a constant definition - no function calls)
- Called from (representative examples):
  - range_table_entry_walker_impl (src/backend/nodes/nodeFuncs.c:2862)
  - inline_cte_walker (src/backend/optimizer/plan/subselect.c:1167)

## Notes and Other Information
- This flag is part of a family of QTW (Query Tree Walker) flags defined in src/include/nodes/nodeFuncs.h
- Can be combined with other QTW flags using bitwise OR operations
- Provides post-order traversal of RTE nodes, complementing the pre-order traversal offered by QTW_EXAMINE_RTES_BEFORE
- Less commonly used than QTW_EXAMINE_RTES_BEFORE, as most operations prefer to examine the RTE structure before diving into contents
- Particularly useful in optimization phases where the results of processing RTE contents influence decisions about the RTE itself
- Essential for maintaining correct dependency order when transforming query trees with nested structures