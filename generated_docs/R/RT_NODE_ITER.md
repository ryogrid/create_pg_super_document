# RT_NODE_ITER

## Location
src/include/lib/radixtree.h: 733 - 743

## Overview
RT_NODE_ITER is a macro that expands to a type name for a structure used to track iteration state within individual nodes of PostgreSQL's radix tree implementation.

## Definition

This expands to a typedef name based on the RT_PREFIX configuration, typically resulting in a structure like .

## Detailed Description
RT_NODE_ITER is part of PostgreSQL's generic radix tree implementation located in radixtree.h. It serves as a type alias that gets expanded through the RT_MAKE_NAME macro system to create prefix-specific type names. The actual structure it refers to contains the state needed to iterate through child nodes within a single radix tree node.

The structure it represents has the following definition:


This iterator is designed to handle different node types in the radix tree (nodes with 4, 16, 48, or 256 children) by tracking the current position differently based on the node structure.

## Parameters / Member Variables
- : A child pointer to the current node being iterated
- : The next index position - for RT_NODE_KIND_4 and RT_NODE_KIND_16 nodes, this is the next index in the chunk array; for RT_NODE_KIND_48 and RT_NODE_KIND_256 nodes, this is the next chunk value. Initial value is 0.

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro expansion system)
- Called from (representative examples):
  - [RT_ITER](RT_ITER.md) (uses RT_NODE_ITER in its node_iters array)
  - [RT_NODE_ITERATE_NEXT](RT_NODE_ITERATE_NEXT.md) (operates on RT_NODE_ITER structures)

## Notes and Other Information
- This is part of PostgreSQL's templated radix tree system that allows multiple instances with different prefixes
- The RT_MAKE_NAME macro system enables type-safe multiple instantiations of the radix tree code
- Used internally by the higher-level RT_ITER structure to maintain a stack of node iterators for multi-level tree traversal
- Essential for implementing efficient tree iteration that can pause and resume at any point