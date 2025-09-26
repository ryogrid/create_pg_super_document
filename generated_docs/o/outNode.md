# outNode

## Location
[src/backend/nodes/outfuncs.c:716-769](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L716-L769)

## Overview
outNode is the main public function that converts PostgreSQL Node objects into their ASCII string representation for serialization and debugging purposes.

## Definition
```c
void outNode(StringInfo str, const void *obj)
```

## Detailed Description
This function serves as the central dispatcher for PostgreSQL's node serialization system. It takes any node object and converts it to its textual representation by determining the node type and calling the appropriate specialized output function. The function handles NULL objects, built-in scalar types (Integer, Float, Boolean, String, BitString), collections (List variants), and complex node types through a comprehensive switch statement. It includes stack overflow protection and provides graceful handling of unrecognized node types with warning messages rather than fatal errors.

## Parameters / Member Variables
- `str`: StringInfo buffer where the node's string representation will be appended
- `obj`: Pointer to the node object to be serialized (can be any PostgreSQL node type or NULL)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - appendStringInfoString (for NULL object representation)
  - IsA (type checking macro)
  - _outList (for List, IntList, OidList, XidList)
  - _outInteger (for Integer nodes)
  - _outFloat (for Float nodes)
  - _outBoolean (for Boolean nodes)
  - _outString (for String nodes)
  - _outBitString (for BitString nodes)
  - outBitmapset (for Bitmapset objects)
  - nodeTag (for node type identification)
  - elog (for warning messages)
- Called from (representative examples):
  - WRITE_NODE_FIELD (macro for serializing node fields)
  - _outList (for recursive list serialization)
  - _outA_Const (for constant value serialization)
  - nodeToStringInternal (main entry point for node-to-string conversion)

## Notes and Other Information
- This is a public function accessible throughout PostgreSQL
- Provides stack overflow protection for deeply nested expressions
- Uses a comprehensive switch statement (via included outfuncs.switch.c) to handle all node types
- Gracefully handles unrecognized node types with warnings rather than errors
- Central component of PostgreSQL's debugging and logging infrastructure
- The serialized format is designed to be human-readable and parseable
- Complex nodes are wrapped in curly braces { } while simple scalar types are not
- Used extensively in query plan visualization, debugging output, and inter-process communication