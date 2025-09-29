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
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - [appendStringInfoString](../a/appendStringInfoString.md) (for NULL object representation)
  - IsA (type checking macro)
  - [_outList](_outList.md) (for List, IntList, OidList, XidList)
  - [_outInteger](_outInteger.md) (for Integer nodes)
  - [_outFloat](_outFloat.md) (for Float nodes)
  - [_outBoolean](_outBoolean.md) (for Boolean nodes)
  - [_outString](_outString.md) (for String nodes)
  - [_outBitString](_outBitString.md) (for BitString nodes)
  - [outBitmapset](outBitmapset.md) (for Bitmapset objects)
  - nodeTag (for node type identification)
  - elog (for warning messages)
- Called from (representative examples):
  - WRITE_NODE_FIELD (macro for serializing node fields)
  - [_outList](_outList.md) (for recursive list serialization)
  - [_outA_Const](_outA_Const.md) (for constant value serialization)
  - [nodeToStringInternal](../n/nodeToStringInternal.md) (main entry point for node-to-string conversion)

## Notes and Other Information
- This is a public function accessible throughout PostgreSQL
- Provides stack overflow protection for deeply nested expressions
- Uses a comprehensive switch statement (via included outfuncs.switch.c) to handle all node types
- Gracefully handles unrecognized node types with warnings rather than errors
- Central component of PostgreSQL's debugging and logging infrastructure
- The serialized format is designed to be human-readable and parseable
- [Complex](../C/Complex.md) nodes are wrapped in curly braces { } while simple scalar types are not
- Used extensively in query plan visualization, debugging output, and inter-process communication

## Simplified Source

```c
void outNode(StringInfo str, const void *obj)
{
    // Prevent stack overflow in deeply nested expressions
    check_stack_depth();

    if (obj == NULL) {
        appendStringInfoString(str, "<>");
        return;
    }

    // Handle list types directly
    if (IsA(obj, List) || IsA(obj, IntList) || IsA(obj, OidList) || IsA(obj, XidList)) {
        _outList(str, obj);
        return;
    }

    // Handle scalar types without braces
    if (IsA(obj, Integer)) {
        _outInteger(str, (Integer *) obj);
    } else if (IsA(obj, Float)) {
        _outFloat(str, (Float *) obj);
    } else if (IsA(obj, Boolean)) {
        _outBoolean(str, (Boolean *) obj);
    } else if (IsA(obj, String)) {
        _outString(str, (String *) obj);
    } else if (IsA(obj, BitString)) {
        _outBitString(str, (BitString *) obj);
    } else if (IsA(obj, Bitmapset)) {
        outBitmapset(str, (Bitmapset *) obj);
    } else {
        // Handle complex node types with braces
        appendStringInfoChar(str, '{');

        switch (nodeTag(obj)) {
            // Comprehensive switch statement handles all node types
            #include "outfuncs.switch.c"

            default:
                // Warn but don't fail on unknown node types
                elog(WARNING, "could not dump unrecognized node type: %d",
                     (int) nodeTag(obj));
                break;
        }

        appendStringInfoChar(str, '}');
    }
}
```