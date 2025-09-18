# RT_GROW_NODE_4

## Location
src/include/lib/radixtree.h: 1479 - 1512

## Overview
A macro that generates the function name for growing a radix tree node-4 to a larger node type when it reaches capacity, enabling dynamic node size expansion in radix trees.

## Definition
```c
#define RT_GROW_NODE_4 RT_MAKE_NAME(grow_node_4)
```

## Detailed Description
RT_GROW_NODE_4 is a macro that expands to a function name responsible for promoting a radix tree node-4 to a larger node type (typically node-16) when the node-4 reaches its maximum capacity of 4 children. This is a critical operation in the adaptive radix tree implementation, allowing the tree to dynamically adjust its internal structure based on the number of children at each node.

The growth operation involves allocating a new larger node, copying existing children from the node-4 to the new node, updating parent pointers, and then adding the new child that triggered the growth. This ensures optimal space utilization while maintaining fast access times.

## Parameters / Member Variables
This macro expands to a function that typically takes:
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent node's slot that references this node
- `node`: Pointer to the node-4 that needs to grow
- `chunk`: The key chunk (byte value) for the new child being added

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
- Called from (representative examples):
  - RT_NODE_INSERT (when RT_NODE_MUST_GROW condition is true for node-4)
- Related symbols:
  - RT_NODE_MUST_GROW (condition check)
  - RT_ADD_CHILD_16 (typically used after growth)

## Notes and Other Information
- Part of PostgreSQL's adaptive radix tree implementation
- Triggered when a node-4's count equals its fanout (4 children)
- Usually promotes node-4 to node-16 for better capacity
- The growth operation is atomic from the tree's perspective
- After growth, the original insertion operation continues with the new larger node
- Critical for maintaining tree balance and preventing unnecessary depth increases
- The actual function implementation handles memory management and pointer updates during the node type transition