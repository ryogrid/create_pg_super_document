# tuplestore_ateof

## Location
[src/backend/utils/sort/tuplestore.c:557-577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L557-L577)

## Overview
Returns whether the currently active read pointer in the tuplestore has reached the end-of-file position.

## Definition

```c
bool
tuplestore_ateof(Tuplestorestate *state)
```
## Detailed Description
This function checks the EOF (end-of-file) status of the currently active read pointer in the tuplestore. Each read pointer maintains an  flag that indicates whether it has read all available tuples and reached the end of the stored data. The function returns the state of this flag for the active read pointer (specified by ).

The EOF state is set when:
- A read operation reaches the end of available tuples
- The read pointer is positioned at the write position (no more tuples to read)
- The tuplestore is empty and a read is attempted

## Parameters / Member Variables
- : Pointer to the  structure containing the tuplestore and read pointers

## Dependencies
- Functions called/Symbols referenced:
  - Uses , , and  fields from  structure
- Called from (representative examples):
  -  (nodeCtescan.c:54)
  -  (nodeMaterial.c:86)

## Notes and Other Information
- Returns a simple boolean value (true if at EOF, false otherwise)
- Very lightweight O(1) operation that just checks a flag
- Essential for loop termination in tuple scanning operations
- Each read pointer maintains its own EOF state independently
- Used primarily in executor nodes that need to know when to stop reading
- The EOF state can be reset if new tuples are added after reaching EOF

## Simplified Source

```c
bool
tuplestore_ateof(Tuplestorestate *state)
{
    // Return EOF status of active read pointer
    return state->readptrs[state->activeptr].eof_reached;
}
```