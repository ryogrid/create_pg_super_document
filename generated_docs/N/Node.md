# Node

## Location
src/include/nodes/nodes.h: 128 - 131

## Overview
The Node struct serves as the base type for all node structures in PostgreSQL's abstract syntax tree (AST) and other tree-based data structures. It provides a common foundation that enables type identification and polymorphic operations across all node types.

## Definition

**File:** src/include/nodes/nodes.h:128-131

## Detailed Description
The Node struct is the fundamental building block of PostgreSQL's node system, implementing a simple form of object-oriented programming in C. Every node structure in PostgreSQL begins with this base Node structure, ensuring that the first field is always a NodeTag that identifies the specific node type.

This design allows PostgreSQL to:
- Perform safe type casting and type checking
- Enable polymorphic operations on different node types
- Provide a consistent interface for tree traversal and manipulation
- Support debugging by allowing any node pointer to be cast to Node* to examine its type

The Node struct works in conjunction with the makeNode() macro for node creation and castNode() macro for type-safe casting operations.

## Parameters / Member Variables
- : A NodeTag enum value that uniquely identifies the specific node type. This field is guaranteed to be the first field in every PostgreSQL node structure, enabling type identification through casting.

## Dependencies
- Types referenced:
  - NodeTag (enum defining all possible node types)
- Related functions/macros:
  - makeNode(): Creates new nodes with proper NodeTag initialization
  - castNode(): Performs type-safe casting with runtime type checking
  - [newNode](../n/newNode.md)(): Low-level node allocation function
  - [castNodeImpl](../c/castNodeImpl.md)(): Implementation function for type checking

## Notes and Other Information
- The Node struct is never used directly but always as the first member of more specific node structures
- All nodes created with makeNode() automatically have their NodeTag field set appropriately
- The NodeTag values are never stored on disk and can change between development versions
- This design pattern is crucial for PostgreSQL's query parsing, planning, and execution systems
- The Node system enables safe polymorphism in a C codebase that handles complex tree structures like parse trees, plan trees, and expression trees
- When debugging, casting any node pointer to (Node*) allows inspection of the node's type field