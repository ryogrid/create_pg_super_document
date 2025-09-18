# ExtensibleNodeMethods

## Location
src/include/nodes/extensible.h: 62 - 73

## Overview
ExtensibleNodeMethods is a structure that defines the required callback functions and metadata for handling a specific type of extensible node in PostgreSQL's node system.

## Definition
```c
typedef struct ExtensibleNodeMethods
{
    const char *extnodename;
    Size        node_size;
    void        (*nodeCopy) (struct ExtensibleNode *newnode,
                            const struct ExtensibleNode *oldnode);
    bool        (*nodeEqual) (const struct ExtensibleNode *a,
                             const struct ExtensibleNode *b);
    void        (*nodeOut) (struct StringInfoData *str,
                           const struct ExtensibleNode *node);
    void        (*nodeRead) (struct ExtensibleNode *node);
} ExtensibleNodeMethods;
```

## Detailed Description
ExtensibleNodeMethods provides the interface definition for extensions to implement custom node handling within PostgreSQL's node system. This structure contains function pointers for all the essential operations needed to integrate custom nodes with PostgreSQL's copy, equality, serialization, and deserialization systems. Extensions must register an instance of this structure to enable their custom nodes to work properly with the core system.

The core system handles the standard fields (type and extnodename) automatically, while the callbacks handle the extension-specific data within the custom node.

## Parameters / Member Variables
- `extnodename`: String identifier that uniquely identifies this node type and matches the extnodename field in ExtensibleNode
- `node_size`: Size in bytes of the complete extensible node structure (including the base ExtensibleNode fields)
- `nodeCopy`: Function pointer for deep copying from oldnode to newnode (excluding type and extnodename which are handled by core)
- `nodeEqual`: Function pointer for deep equality comparison between two nodes, returning true/false (excluding type and extnodename)
- `nodeOut`: Function pointer for serializing the node using outfuncs.c conventions (excluding type and extnodename)
- `nodeRead`: Function pointer for deserializing the node using readfuncs.c conventions with pg_strtok() (excluding type and extnodename)

## Dependencies
- Functions called/Symbols referenced:
  - ExtensibleNode (referenced in callback signatures)
  - Size (for node_size field)
  - StringInfoData (for nodeOut callback)
  - nodeRead (function pointer type)
- Called from (representative examples):
  - RegisterExtensibleNodeMethods (registration function)
  - GetExtensibleNodeMethods (lookup function)
  - _copyExtensibleNode (copy operations)
  - _equalExtensibleNode (equality operations)
  - _outExtensibleNode (output operations)
  - _readExtensibleNode (read operations)

## Notes and Other Information
- All callback functions are mandatory - extensions must provide implementations for all of them
- The extnodename must be unique across all registered extensible node types
- Extensions typically register their ExtensibleNodeMethods during module initialization
- The callbacks only handle extension-specific data; the core system manages the base ExtensibleNode fields
- This design allows extensions to seamlessly integrate with PostgreSQL's node copying, comparison, and serialization infrastructure
- Function pointers enable polymorphic behavior while maintaining type safety within the C type system