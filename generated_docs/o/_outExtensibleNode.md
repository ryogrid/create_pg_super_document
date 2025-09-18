# _outExtensibleNode

## Location
[src/backend/nodes/outfuncs.c:481-495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L481-L495)

## Overview
Serializes an ExtensibleNode structure to a string format for output operations, handling both the standard node metadata and custom private fields through extensible node methods.

## Definition


## Detailed Description
The  function is part of PostgreSQL's node output system, specifically designed to serialize ExtensibleNode structures. ExtensibleNodes are a mechanism in PostgreSQL that allows external extensions to define their own node types while integrating seamlessly with the core parser and planner infrastructure.

The function first retrieves the appropriate method structure for the specific extensible node type using . It then writes the standard node type identifier "EXTENSIBLENODE" and the extension name to the output string. Finally, it delegates the serialization of private/custom fields to the extension-specific  callback method, allowing each extension to control how its private data is serialized.

## Parameters / Member Variables
- : StringInfo buffer where the serialized node representation will be written
- : Pointer to the ExtensibleNode structure to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - [GetExtensibleNodeMethods](../G/GetExtensibleNodeMethods.md)
  - WRITE_NODE_TYPE
  - WRITE_STRING_FIELD
- Types referenced:
  - [ExtensibleNode](../E/ExtensibleNode.md)
  - [ExtensibleNodeMethods](../E/ExtensibleNodeMethods.md)
- Called from (representative examples):
  - No direct callers found (likely called through function pointer dispatch in the node output system)

## Notes and Other Information
- This is a static function, indicating it's only used within the outfuncs.c compilation unit
- The function assumes that the ExtensibleNodeMethods structure contains a valid  callback function
- Part of PostgreSQL's extensibility framework that allows third-party extensions to define custom node types
- The serialized output follows PostgreSQL's standard node format with type identification followed by field data