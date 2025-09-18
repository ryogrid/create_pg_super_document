# QTW_DONT_COPY_QUERY

## Location
src/include/nodes/nodeFuncs.h: 31 - 31

## Overview
A flag bit constant used to control the behavior of query_tree_mutator functions, instructing them to avoid copying the top-level Query node during tree mutation operations.

## Definition


## Detailed Description
QTW_DONT_COPY_QUERY is a bit flag with value 0x40 (64 in decimal) that modifies the behavior of query tree mutation operations. When this flag is set, the query_tree_mutator will not create a copy of the top-level Query node during its traversal and mutation process. This is an optimization flag that can be used when the caller knows that the top-level Query structure itself doesn't need to be modified, only its contents.

This flag is particularly useful in scenarios where multiple mutation passes are being performed on the same query tree, and intermediate passes don't need to modify the Query node structure itself, only the expressions and other components within it. By avoiding unnecessary copying of the Query node, this flag can improve performance and reduce memory allocation overhead.

## Parameters / Member Variables
- Value:  (hexadecimal) - The bit flag value used in bitwise operations with other QTW flags

## Dependencies
- Functions called/Symbols referenced:
  - (This is a constant definition - no function calls)
- Called from (representative examples):
  - [query_tree_mutator_impl](../q/query_tree_mutator_impl.md) (src/backend/nodes/nodeFuncs.c:3757)

## Notes and Other Information
- This flag is part of a family of QTW (Query Tree Walker) flags defined in src/include/nodes/nodeFuncs.h
- Can be combined with other QTW flags using bitwise OR operations
- Specifically affects query_tree_mutator behavior, not query_tree_walker
- Used as a performance optimization to avoid unnecessary node copying
- Should be used with caution - only when the caller is certain the top Query node won't need modification
- Helps reduce memory allocation and copying overhead in multi-pass query transformation scenarios
- The flag affects only the top-level Query node; nested Query nodes in subqueries may still be copied as needed