# GetExtensibleNodeMethods

## Location
src/backend/nodes/extensible.c: 125 - 136

## Overview
Retrieves the method structure for a given type of extensible node, providing access to the callbacks needed for node operations.

## Definition
```c
const ExtensibleNodeMethods *GetExtensibleNodeMethods(const char *extnodename, bool missing_ok)
```

## Detailed Description
This function serves as the public API for looking up extensible node methods in PostgreSQL. It acts as a wrapper around the internal GetExtensibleNodeEntry function, specifically targeting the global extensible_node_methods hash table. The function returns a pointer to the ExtensibleNodeMethods structure that was previously registered for the given node type name. This allows the PostgreSQL node processing system to access the appropriate callback functions for copying, comparison, serialization, and other operations on extensible nodes.

## Parameters / Member Variables
- `extnodename`: Name identifier of the extensible node type to look up
- `missing_ok`: If true, returns NULL for unregistered node types; if false, raises ERROR for missing types

## Dependencies
- Functions called/Symbols referenced:
  - [GetExtensibleNodeEntry](GetExtensibleNodeEntry.md)
- Data types used:
  - [ExtensibleNodeMethods](../E/ExtensibleNodeMethods.md)
- Called from (representative examples):
  - [_copyExtensibleNode](../c/_copyExtensibleNode.md)
  - [_equalExtensibleNode](../e/_equalExtensibleNode.md)
  - [_outExtensibleNode](../o/_outExtensibleNode.md)
  - [_readExtensibleNode](../r/_readExtensibleNode.md)

## Notes and Other Information
- This is the primary lookup function used by PostgreSQL's node processing infrastructure
- Used by copy, equality, output, and input functions for extensible nodes
- Returns const pointer to prevent modification of registered method structures
- Leverages the global extensible_node_methods hash table for lookups
- Critical for proper functioning of extensible nodes in query plans and other contexts
- The missing_ok parameter allows callers to handle unregistered node types gracefully
- Commonly called during node tree traversal and manipulation operations