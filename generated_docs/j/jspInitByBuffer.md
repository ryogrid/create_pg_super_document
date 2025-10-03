# jspInitByBuffer

## Location
[src/backend/utils/adt/jsonpath.c:983-1073](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L983-L1073)

## Overview
Initializes a JsonPathItem structure by reading and parsing JSON path node data from a buffer at a specified position, handling all supported JSON path item types.

## Definition

```c
void
jspInitByBuffer(JsonPathItem *v, char *base, int32 pos)
```
## Detailed Description
This function is the core initialization routine for JsonPathItem structures, responsible for parsing the binary representation of JSON path nodes from a buffer. It reads the node type, aligns the position for proper data alignment, reads the next position offset, and then processes type-specific data based on a comprehensive switch statement. The function handles all JSON path item types including literals, operators, functions, and complex constructs like arrays and regex patterns. Each node type has specific data layout requirements that this function properly interprets and loads into the JsonPathItem structure.

## Parameters / Member Variables
- `*v`: Pointer to the JsonPathItem structure to be initialized with the parsed node data
- `*base`: Pointer to the beginning of the buffer containing the JSON path binary data
- `pos`: Starting position (offset) within the buffer where the node data begins
## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItem (struct type)
  - read_byte, read_int32, read_int32_n (buffer reading macros/functions)
  - INTALIGN (alignment macro)
  - All jpi* enumeration constants for different node types
  - elog (for error reporting on unrecognized types)
- Called from (representative examples):
  - [printJsonPathItem](../p/printJsonPathItem.md)
  - [jspInit](jspInit.md)
  - [jspGetArg](jspGetArg.md)
  - [jspGetNext](jspGetNext.md)
  - [jspGetLeftArg](jspGetLeftArg.md), jspGetRightArg
  - [jspGetArraySubscript](jspGetArraySubscript.md)
  - [jspIsMutableWalker](jspIsMutableWalker.md)

## Notes and Other Information
- Handles proper memory alignment using INTALIGN for cross-platform compatibility
- Supports complex data structures like arrays (jpiIndexArray), regex patterns (jpiLikeRegex), and bounded ranges (jpiAny)
- Different node types require different amounts and types of additional data beyond the basic type and nextPos fields
- Essential building block for all JSON path traversal and evaluation operations
- Error handling for unrecognized node types prevents corruption from invalid data
- The function directly manipulates buffer pointers to access variable-length data efficiently

## Simplified Source

```c
void jspInitByBuffer(JsonPathItem *v, char *base, int32 pos) {
    // Set base pointer for this item
    v->base = base + pos;

    // Read node type and align position
    read_byte(v->type, base, pos);
    pos = INTALIGN((uintptr_t) (base + pos)) - (uintptr_t) base;
    read_int32(v->nextPos, base, pos);

    // Process type-specific data
    switch (v->type) {
        // Simple node types with no additional data
        case jpiNull:
        case jpiRoot:
        case jpiCurrent:
        case jpiAnyArray:
        case jpiAnyKey:
        case jpiType:
        case jpiSize:
        case jpiAbs:
        case jpiFloor:
        case jpiCeiling:
        case jpiDouble:
        case jpiKeyValue:
        case jpiLast:
        case jpiBigint:
        case jpiBoolean:
        case jpiDate:
        case jpiInteger:
        case jpiNumber:
        case jpiStringFunc:
            break;

        // String/key/variable types with length and data
        case jpiString:
        case jpiKey:
        case jpiVariable:
            read_int32(v->content.value.datalen, base, pos);
            // Fall through to set data pointer
        case jpiNumeric:
        case jpiBool:
            v->content.value.data = base + pos;
            break;

        // Binary operators with left and right arguments
        case jpiAnd:
        case jpiOr:
        case jpiEqual:
        case jpiNotEqual:
        case jpiLess:
        case jpiGreater:
        case jpiLessOrEqual:
        case jpiGreaterOrEqual:
        case jpiAdd:
        case jpiSub:
        case jpiMul:
        case jpiDiv:
        case jpiMod:
        case jpiStartsWith:
        case jpiDecimal:
            read_int32(v->content.args.left, base, pos);
            read_int32(v->content.args.right, base, pos);
            break;

        // Unary operators with single argument
        case jpiNot:
        case jpiIsUnknown:
        case jpiExists:
        case jpiPlus:
        case jpiMinus:
        case jpiFilter:
        case jpiDatetime:
        case jpiTime:
        case jpiTimeTz:
        case jpiTimestamp:
        case jpiTimestampTz:
            read_int32(v->content.arg, base, pos);
            break;

        // Array indexing with element count and indices
        case jpiIndexArray:
            read_int32(v->content.array.nelems, base, pos);
            read_int32_n(v->content.array.elems, base, pos, v->content.array.nelems * 2);
            break;

        // Range bounds for "any" expressions
        case jpiAny:
            read_int32(v->content.anybounds.first, base, pos);
            read_int32(v->content.anybounds.last, base, pos);
            break;

        // Regular expression with flags, expression, and pattern
        case jpiLikeRegex:
            read_int32(v->content.like_regex.flags, base, pos);
            read_int32(v->content.like_regex.expr, base, pos);
            read_int32(v->content.like_regex.patternlen, base, pos);
            v->content.like_regex.pattern = base + pos;
            break;

        default:
            elog(ERROR, "unrecognized jsonpath item type: %d", v->type);
    }
}
```