# _jumbleNode

## Location
src/backend/nodes/queryjumblefuncs.c: 236 - 291

## Overview
The `_jumbleNode` function is a core recursive function in PostgreSQL's query jumbling system that processes AST (Abstract Syntax Tree) nodes to generate normalized query fingerprints for query plan caching and statistics.

## Definition
```c
static void _jumbleNode(JumbleState *jstate, Node *node)
```

## Detailed Description
The `_jumbleNode` function is the central engine of PostgreSQL's query jumbling mechanism, responsible for traversing and processing individual nodes in the query's abstract syntax tree. It systematically processes each node by:

1. **Stack overflow protection**: Guards against deeply nested expressions using `check_stack_depth()`
2. **Node type emission**: Always emits the node's NodeTag to the jumble state
3. **Node-specific processing**: Handles different node types through an included switch statement from `queryjumblefuncs.switch.c`
4. **List handling**: Special handling for List-type nodes (T_List, T_IntList, T_OidList, T_XidList) by calling `_jumbleList`
5. **Parameter tracking**: Special case handling for T_Param nodes to track the highest external parameter ID

The function operates recursively, ensuring that all child nodes are properly processed to create a complete query fingerprint.

## Parameters / Member Variables
- `jstate`: JumbleState pointer containing the current jumbling state and accumulated jumble data
- `node`: Node pointer to the AST node being processed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `check_stack_depth` - Stack overflow protection
  - `JUMBLE_FIELD` - Macro to jumble node fields
  - `nodeTag` - Get the NodeTag type of a node
  - [_jumbleList](_jumbleList.md) - Process list-type nodes
  - `elog` - Logging for unrecognized node types
- Called from (representative examples):
  - [JumbleQuery](../J/JumbleQuery.md) - Main query jumbling entry point
  - `JUMBLE_NODE` - Macro wrapper for jumbling nodes
  - [_jumbleList](_jumbleList.md) - Recursive calls from list processing

## Notes and Other Information
- The function includes an automated switch statement from `queryjumblefuncs.switch.c` which is generated to handle the vast majority of PostgreSQL node types
- Special handling exists for Param nodes to track parameter normalization requirements
- The function emits warnings but continues processing for unrecognized node types, making it resilient to new node types
- This is a key component of PostgreSQL's query plan caching mechanism, enabling efficient identification of equivalent queries with different literal values