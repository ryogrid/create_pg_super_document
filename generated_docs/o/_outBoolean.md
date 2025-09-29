# _outBoolean

## Location
[src/backend/nodes/outfuncs.c:664-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L664-L669)

## Overview
_outBoolean is a static helper function that serializes a Boolean node to its string representation in PostgreSQL's node output format.

## Definition

```c
static void
_outBoolean(StringInfo str, const Boolean *node)
```
## Detailed Description
This function converts a Boolean node into its textual representation by appending either "true" or "false" to the provided StringInfo buffer based on the boolean value stored in the node. It provides a simple and direct serialization mechanism for Boolean constants in PostgreSQL's abstract syntax tree representation.

## Parameters / Member Variables
- : StringInfo buffer where the boolean string representation will be appended
- : Pointer to the Boolean node containing the boolean value to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoString](../a/appendStringInfoString.md) (indirectly via string literal appending)
  - [Boolean](../B/Boolean.md) (node type)
- Called from (representative examples):
  - [outNode](outNode.md) (main node serialization dispatcher)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the outfuncs.c file
- The function directly accesses the boolval field of the Boolean node structure
- Part of PostgreSQL's node serialization system used for debugging, logging, and inter-process communication
- The output format matches the expected input format for the corresponding node reading functions

## Simplified Source

```c
static void
_outBoolean(StringInfo str, const Boolean *node)
{
    // Simply output "true" or "false" based on the boolean value
    appendStringInfoString(str, node->boolval ? "true" : "false");
}
```