# _copyExtensibleNode

## Location
src/backend/nodes/copyfuncs.c: 147 - 163

## Overview
Creates a deep copy of an ExtensibleNode by delegating the node-specific copying logic to the registered copy method for that extensible node type.

## Definition


## Detailed Description
The  function provides a generic copying mechanism for ExtensibleNode structures, which are a framework in PostgreSQL for creating custom node types that can be plugged into the existing node infrastructure. The function follows a two-phase approach: first, it allocates a new node of the appropriate size using the node's registered methods, then delegates the actual field copying to the node type's custom copy function.

This design allows extensible nodes to define their own copying semantics while still participating in PostgreSQL's generic node copying system. The function retrieves the node's methods using  and uses the custom  callback to handle the type-specific copying of private fields.

## Parameters / Member Variables
- : Pointer to the source ExtensibleNode to be copied

## Dependencies
- Functions called/Symbols referenced:
  - [GetExtensibleNodeMethods](../G/GetExtensibleNodeMethods.md) (retrieves method structure for the extensible node type)
  - [newNode](../n/newNode.md) (allocates a new node with specified size and type)
  - COPY_STRING_FIELD (macro for copying the extension node name)
  - methods->nodeCopy (custom copy function provided by the extensible node implementation)
- Called from (representative examples):
  - Part of the node copying system (called indirectly through copyObject)

## Notes and Other Information
- This is a static function, only accessible within copyfuncs.c
- [ExtensibleNode](../E/ExtensibleNode.md) provides a plugin mechanism for custom node types in PostgreSQL
- The function demonstrates the delegation pattern - generic framework code delegates to type-specific implementations
- The  field is used as a key to look up the appropriate methods for the specific extensible node type
- This approach allows third-party extensions to define custom node types that integrate seamlessly with PostgreSQL's node infrastructure