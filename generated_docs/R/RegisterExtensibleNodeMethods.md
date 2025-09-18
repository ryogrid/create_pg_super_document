# RegisterExtensibleNodeMethods

## Location
[src/backend/nodes/extensible.c:76-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/extensible.c#L76-L87)

## Overview
Registers a new type of extensible node in PostgreSQL's extensible node system, making it available for use throughout the system.

## Definition
```c
void RegisterExtensibleNodeMethods(const ExtensibleNodeMethods *methods)
```

## Detailed Description
This function serves as the public API for registering extensible node types in PostgreSQL. It acts as a wrapper around the internal RegisterExtensibleNodeEntry function, specifically for extensible nodes (as opposed to custom scan methods). The function registers the provided method structure in the global extensible_node_methods hash table, using the node's name from the methods structure as the key. This registration makes the extensible node type available for creation, copying, and other operations throughout the PostgreSQL system.

## Parameters / Member Variables
- `methods`: Pointer to ExtensibleNodeMethods structure containing the node type name and callback functions for this extensible node type

## Dependencies
- Functions called/Symbols referenced:
  - [RegisterExtensibleNodeEntry](RegisterExtensibleNodeEntry.md)
- Data types used:
  - [ExtensibleNodeMethods](../E/ExtensibleNodeMethods.md)
- Called from (representative examples):
  - (No direct references found - likely called from extension modules)

## Notes and Other Information
- This is the primary entry point for extensions to register new node types
- Uses the global extensible_node_methods hash table for storage
- The methods parameter must contain a valid extnodename field
- Extensions typically call this function during module initialization
- The registered methods become available system-wide once registration completes
- Registration is permanent for the duration of the PostgreSQL session