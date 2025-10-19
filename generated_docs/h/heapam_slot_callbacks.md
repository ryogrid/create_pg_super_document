# heapam_slot_callbacks

## Location
[src/backend/access/heap/heapam_handler.c:68-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L68-L79)

## Overview
Returns the tuple table slot operations structure for heap access method relations, providing the appropriate slot callbacks for heap tuples stored in buffer pages.

## Definition

```c
static const TupleTableSlotOps *
heapam_slot_callbacks(Relation relation)
```
## Detailed Description
This function is part of PostgreSQL's table access method (TAM) infrastructure. It serves as a callback function within the heap access method handler to return the appropriate tuple table slot operations for heap relations. The function returns a pointer to , which contains the set of operations optimized for heap tuples that reside in shared buffer pages. This is a key component of the pluggable storage engine architecture, allowing different storage formats to provide their own slot operation implementations while maintaining a consistent interface.

## Parameters / Member Variables
- `relation`: A Relation pointer representing the heap relation for which slot callbacks are requested
## Dependencies
- Functions called/Symbols referenced:
  - TTSOpsBufferHeapTuple (global tuple table slot operations structure)
- Called from (representative examples):
  - Part of TableAmRoutine structure as a callback function
  - Used by SampleHeapTupleVisible

## Notes and Other Information
- This is a static function within the heap access method handler
- The function is simple but crucial for the table access method abstraction
- Returns operations specifically designed for buffer-resident heap tuples
- Part of the broader heapam (heap access method) handler implementation
- The returned TTSOpsBufferHeapTuple provides optimized operations for heap tuples stored in PostgreSQL's shared buffer pool

## Simplified Source

```c
static const TupleTableSlotOps *heapam_slot_callbacks(Relation relation) {
    // Return the tuple slot operations optimized for buffer-resident heap tuples
    return &TTSOpsBufferHeapTuple;
}
```