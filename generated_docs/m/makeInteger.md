# makeInteger

## Location
[src/backend/nodes/value.c:23-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/value.c#L23-L36)

## Overview
The makeInteger function creates a new Integer node containing a specified integer value, used for representing integer literals in PostgreSQL's parse tree structure.

## Definition

```c
Integer *
makeInteger(int i)
```
## Detailed Description
makeInteger is a factory function that allocates and initializes a new Integer node in PostgreSQL's node system. It uses the makeNode macro to create a properly initialized node with the correct NodeTag, then sets the integer value. This function is part of PostgreSQL's value node system, which allows primitive types like integers to be stored in parse trees and expression trees as proper nodes that can be manipulated by the node system.

The Integer node type is specifically designed to represent integer literals found during parsing and to pass integer constants throughout the parser and planner. Unlike plain int values, Integer nodes can be stored in PostgreSQL's List structures and participate in the node copying, serialization, and other node operations.

## Parameters / Member Variables
- : The integer value to store in the newly created Integer node

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (macro for node allocation and initialization)
  - [Integer](../I/Integer.md) (struct type definition)
- Called from (representative examples):
  - [buildDefItem](../b/buildDefItem.md) (in tsearchcmds.c)
  - [nodeRead](../n/nodeRead.md) (in read.c for deserialization)
  - strVal (referenced in value.h header)

## Notes and Other Information
- Part of PostgreSQL's value node system alongside makeFloat, makeString, and makeBitString
- The Integer node can be put into PostgreSQL Lists, unlike plain int values
- Used extensively in parser and lexer for representing integer literals
- The function is simple but essential for maintaining type safety in the node system
- Located in src/backend/nodes/value.c as part of the core node manipulation functions

## Simplified Source

```c
Integer *makeInteger(int i)
{
    Integer *v = makeNode(Integer);
    v->ival = i;
    return v;
}
```