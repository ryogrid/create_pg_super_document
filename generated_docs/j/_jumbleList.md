# _jumbleList

## Location
[src/backend/nodes/queryjumblefuncs.c:292-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/queryjumblefuncs.c#L292-L322)

## Overview
The `_jumbleList` function is a specialized helper function in PostgreSQL's query jumbling system that processes List-type nodes, handling different kinds of lists (generic lists, integer lists, OID lists, and transaction ID lists) appropriately.

## Definition
```c
static void _jumbleList(JumbleState *jstate, Node *node)
```

## Detailed Description
The `_jumbleList` function handles the jumbling of PostgreSQL's List data structure and its variants. It processes different types of lists based on their specific type tag:

1. **T_List**: Generic lists containing Node pointers - recursively calls `_jumbleNode` for each element
2. **T_IntList**: Lists of integers - directly jumbles each integer value using `JUMBLE_FIELD_SINGLE`
3. **T_OidList**: Lists of object identifiers - directly jumbles each OID value
4. **T_XidList**: Lists of transaction IDs - directly jumbles each XID value

The function ensures that the order and content of list elements are properly reflected in the query jumble, while handling the type-specific extraction methods for each list variant.

## Parameters / Member Variables
- `jstate`: JumbleState pointer containing the current jumbling state and accumulated jumble data
- `node`: Node pointer to the List node being processed (cast to List*)

## Dependencies
- Functions called/Symbols referenced:
  - [_jumbleNode](_jumbleNode.md) - Recursively process Node elements in generic lists
  - `JUMBLE_FIELD_SINGLE` - Macro to jumble single scalar values
  - `lfirst` - Extract Node pointer from list cell (implicit in foreach)
  - `lfirst_int` - Extract integer value from list cell
  - `lfirst_oid` - Extract OID value from list cell  
  - `lfirst_xid` - Extract XID value from list cell
  - `elog` - Error logging for unrecognized list types
- Called from (representative examples):
  - [_jumbleNode](_jumbleNode.md) - When processing T_List, T_IntList, T_OidList, or T_XidList nodes

## Notes and Other Information
- The function uses a switch statement to handle the four supported list types in PostgreSQL
- For generic T_List nodes, it recursively processes each element as a Node, allowing for nested complex structures
- For typed lists (Int, Oid, Xid), it directly jumbles the scalar values for efficiency
- Unlike `_jumbleNode`, this function throws an ERROR (not just a warning) for unrecognized list types, as list types are more constrained and controlled
- The function leverages PostgreSQL's `foreach` macro for safe list iteration across all list types

## Simplified Source

```c
static void
_jumbleList(JumbleState *jstate, Node *node)
{
    List       *expr = (List *) node;
    ListCell   *l;

    switch (expr->type) {
        case T_List:
            // Generic lists - recursively jumble each Node element
            foreach(l, expr)
                _jumbleNode(jstate, lfirst(l));
            break;
        case T_IntList:
            // Integer lists - directly jumble each int value
            foreach(l, expr)
                JUMBLE_FIELD_SINGLE(lfirst_int(l));
            break;
        case T_OidList:
            // OID lists - directly jumble each OID value
            foreach(l, expr)
                JUMBLE_FIELD_SINGLE(lfirst_oid(l));
            break;
        case T_XidList:
            // XID lists - directly jumble each XID value
            foreach(l, expr)
                JUMBLE_FIELD_SINGLE(lfirst_xid(l));
            break;
        default:
            elog(ERROR, "unrecognized list node type: %d", (int) expr->type);
            return;
    }
}
```