# get_assignment_input

## Location
[src/backend/rewrite/rewriteHandler.c:1189-1217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1189-L1217)

## Overview
Returns the input expression from assignment nodes (FieldStore or SubscriptingRef), or NULL if the node is not an assignment operation.

## Definition
```c
static Node *get_assignment_input(Node *node)
```

## Detailed Description
This utility function examines a given node and extracts the input expression if the node represents an assignment operation. It specifically handles two types of assignment nodes in PostgreSQL's expression tree:

1. **FieldStore nodes**: Used for assignments to struct/composite type fields (e.g., `col.field = value`)
2. **SubscriptingRef nodes**: Used for array element assignments (e.g., `arr[1] = value`)

The function is essential for the rewrite system to identify and validate the source expressions in complex assignment operations, particularly when handling multiple assignments to the same column attribute.

For FieldStore nodes, it returns the `arg` field which represents the input expression being modified. For SubscriptingRef nodes, it first verifies that there is actually an assignment expression (`refassgnexpr` is not NULL) before returning the `refexpr` field.

## Parameters / Member Variables
- `node`: The expression node to examine for assignment input

## Dependencies
- Functions called/Symbols referenced:
  - [FieldStore](../F/FieldStore.md)
  - [SubscriptingRef](../S/SubscriptingRef.md)
- Called from (representative examples):
  - [process_matched_tle](../p/process_matched_tle.md)

## Notes and Other Information
- Returns NULL for non-assignment nodes or NULL input
- For SubscriptingRef, only returns input if there's an actual assignment expression
- Critical for validating that multiple assignments target the same base expression
- Used in the rewrite phase to ensure assignment operation compatibility

## Simplified Source

```c
static Node *
get_assignment_input(Node *node)
{
    if (node == NULL)
        return NULL;

    // Handle field assignments (e.g., record.field = value)
    if (IsA(node, FieldStore)) {
        FieldStore *fstore = (FieldStore *) node;
        return (Node *) fstore->arg;
    }

    // Handle array assignments (e.g., arr[1] = value)
    else if (IsA(node, SubscriptingRef)) {
        SubscriptingRef *sbsref = (SubscriptingRef *) node;

        // Only return input if this is actually an assignment
        if (sbsref->refassgnexpr == NULL)
            return NULL;

        return (Node *) sbsref->refexpr;
    }

    return NULL;
}
```