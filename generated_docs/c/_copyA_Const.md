# _copyA_Const

## Location
[src/backend/nodes/copyfuncs.c:108-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/copyfuncs.c#L108-L146)

## Overview
Creates a deep copy of an A_Const node, handling different constant value types (integer, float, boolean, string, bit string) with type-specific copying logic.

## Definition

```c
static A_Const *
_copyA_Const(const A_Const *from)
```
## Detailed Description
The  function is a specialized copy function for A_Const nodes, which represent constants in the abstract syntax tree before type resolution. Unlike Const nodes which are used in the executor, A_Const nodes are used during parsing and analysis phases. The function handles the polymorphic nature of A_Const by using a switch statement based on the node type tag to determine how to copy the underlying value union.

The function only copies the value if it's not null, and uses different copying strategies depending on the value type: scalar copying for integers and booleans, string copying for floats, strings, and bit strings. This ensures that string values are properly duplicated in memory rather than just copying pointers.

## Parameters / Member Variables
- `*from`: Pointer to the source A_Const node to be copied
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create new A_Const node)
  - COPY_SCALAR_FIELD (macro for copying scalar fields)
  - COPY_STRING_FIELD (macro for copying string fields with proper allocation)
  - nodeTag (to determine the type of the value union)
  - COPY_LOCATION_FIELD (macro for copying location information)
  - elog (for error reporting on unrecognized node types)
- Called from (representative examples):
  - Part of the node copying system (called indirectly through copyObject)

## Notes and Other Information
- This is a static function, only accessible within copyfuncs.c
- [A_Const](../A/A_Const.md) is used for constants in parse trees before type analysis, while Const is used after type resolution
- The function handles a union of different value types (Integer, Float, Boolean, String, BitString)
- Error handling is included for unrecognized node types, making the function robust against corruption
- The switch statement approach allows for efficient type-specific handling of the polymorphic value field

## Simplified Source

```c
static A_Const *
_copyA_Const(const A_Const *from)
{
    A_Const *newnode = makeNode(A_Const);

    // Copy the null flag
    COPY_SCALAR_FIELD(isnull);

    if (!from->isnull) {
        // Copy the value based on its type
        COPY_SCALAR_FIELD(val.node.type);

        switch (nodeTag(&from->val)) {
            case T_Integer:
                COPY_SCALAR_FIELD(val.ival.ival);
                break;
            case T_Float:
                COPY_STRING_FIELD(val.fval.fval);
                break;
            case T_Boolean:
                COPY_SCALAR_FIELD(val.boolval.boolval);
                break;
            case T_String:
                COPY_STRING_FIELD(val.sval.sval);
                break;
            case T_BitString:
                COPY_STRING_FIELD(val.bsval.bsval);
                break;
            default:
                elog(ERROR, "unrecognized node type: %d",
                     (int) nodeTag(&from->val));
        }
    }

    // Copy location information
    COPY_LOCATION_FIELD(location);

    return newnode;
}
```