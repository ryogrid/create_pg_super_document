# makeBoolean

## Location
[src/backend/nodes/value.c:49-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/value.c#L49-L62)

## Overview
The makeBoolean function creates a new Boolean node containing a specified boolean value, used for representing boolean literals in PostgreSQL's parse tree structure.

## Definition
Boolean *makeBoolean(bool val)

## Detailed Description
makeBoolean is a factory function that allocates and initializes a new Boolean node in PostgreSQL's node system. It uses the makeNode macro to create a properly initialized node with the correct NodeTag, then sets the boolean value. This function is part of PostgreSQL's value node system, which allows primitive types like booleans to be stored in parse trees and expression trees as proper nodes that can be manipulated by the node system.

The Boolean node type is specifically designed to represent boolean literals (TRUE/FALSE) found during parsing and to pass boolean constants throughout the parser and planner. Like other value nodes, Boolean nodes can be stored in PostgreSQL's List structures and participate in standard node operations such as copying, serialization, and tree traversal.

## Parameters / Member Variables
- `val`: The boolean value (true or false) to store in the newly created Boolean node

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (macro for node allocation and initialization)
  - [Boolean](../B/Boolean.md) (struct type definition)
- Called from (representative examples):
  - [sequence_options](../s/sequence_options.md) (in sequence.c)
  - [buildDefItem](../b/buildDefItem.md) (in tsearchcmds.c, multiple locations)
  - [nodeRead](../n/nodeRead.md) (in read.c for deserialization)

## Notes and Other Information
- Part of PostgreSQL's value node system alongside makeInteger, makeFloat, makeString, and makeBitString
- Enables boolean values to participate in the node system and be stored in Lists
- Used extensively for representing boolean literals and constants in parse trees
- Simple but essential for maintaining type consistency in PostgreSQL's node-based architecture
- The underlying bool type follows standard C boolean semantics
- Located in src/backend/nodes/value.c as part of the core value node creation functions

## Simplified Source

```c
Boolean *makeBoolean(bool val)
{
    Boolean *v = makeNode(Boolean);
    v->boolval = val;
    return v;
}
```