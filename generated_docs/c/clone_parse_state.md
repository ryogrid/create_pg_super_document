# clone_parse_state

## Location
[src/backend/utils/adt/jsonb.c:1471-1500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1471-L1500)

## Overview
Creates a shallow clone of a JsonbParseState structure, primarily used in aggregate final functions for safe state manipulation.

## Definition

```c
static JsonbParseState *
clone_parse_state(JsonbParseState *state)
```
## Detailed Description
The  function creates a shallow copy of a JsonbParseState linked list structure. This function is specifically designed for use in aggregate final functions where the parse state needs to be modified (typically by appending values) without affecting the original state. The cloning process traverses the entire linked list of parse states and copies each node's essential fields, creating an independent copy suitable for final processing.

## Parameters / Member Variables
- `*state`: Pointer to the JsonbParseState to be cloned; returns NULL if input is NULL
## Dependencies
- Functions called/Symbols referenced:
  -  - Allocate memory for new parse state nodes
  -  - Structure type being cloned
- Called from:
  -  (src/backend/utils/adt/jsonb.c:1662)
  -  (src/backend/utils/adt/jsonb.c:1953)

## Notes and Other Information
- Performs shallow cloning - copies structure fields but not deep data references
- Handles linked list traversal to clone entire parse state chain
- Designed specifically for aggregate final functions that append rather than modify existing values
- Returns NULL for NULL input, maintaining null-safety
- Each node in the linked list is individually allocated and copied
- Essential for maintaining isolation between aggregate processing phases

## Simplified Source

```c
static JsonbParseState *clone_parse_state(JsonbParseState *state) {
    JsonbParseState *result, *icursor, *ocursor;

    // Return NULL for NULL input
    if (state == NULL) {
        return NULL;
    }

    // Allocate first node and set up cursors
    result = palloc(sizeof(JsonbParseState));
    icursor = state;
    ocursor = result;

    // Clone each node in the linked list
    for (;;) {
        // Copy essential fields
        ocursor->contVal = icursor->contVal;
        ocursor->size = icursor->size;
        ocursor->unique_keys = icursor->unique_keys;
        ocursor->skip_nulls = icursor->skip_nulls;

        // Move to next node
        icursor = icursor->next;
        if (icursor == NULL) {
            break;
        }

        // Allocate next node and advance output cursor
        ocursor->next = palloc(sizeof(JsonbParseState));
        ocursor = ocursor->next;
    }

    // Terminate the list
    ocursor->next = NULL;

    return result;
}
```