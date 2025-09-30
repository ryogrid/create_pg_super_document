# CopyMultiInsertInfoIsFull

## Location
[src/backend/commands/copyfrom.c:283-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L283-L294)

## Overview
CopyMultiInsertInfoIsFull checks whether the multi-insert buffers have reached their capacity limits, returning true if either the tuple count or byte size thresholds have been exceeded.

## Definition

```c
static inline bool
CopyMultiInsertInfoIsFull(CopyMultiInsertInfo *miinfo)
```
## Detailed Description
This function serves as a capacity check for the multi-insert buffering system in COPY FROM operations. It implements a dual-threshold approach to determine when buffers should be flushed:

1. **Tuple count threshold**: Checks if the number of buffered tuples has reached or exceeded MAX_BUFFERED_TUPLES
2. **Memory threshold**: Checks if the total buffered bytes has reached or exceeded MAX_BUFFERED_BYTES

The function returns true if either threshold is met, signaling that it's time to flush the accumulated tuples to storage. This dual-threshold approach ensures that the system doesn't consume excessive memory (controlled by MAX_BUFFERED_BYTES) while also preventing the buffer from growing too large in terms of tuple count (controlled by MAX_BUFFERED_TUPLES).

The function is typically called before adding new tuples to the buffer to determine whether a flush operation should be performed first. This helps maintain optimal performance by balancing memory usage with the efficiency gains from batched insertions.

## Parameters / Member Variables
- : Pointer to CopyMultiInsertInfo structure containing the current buffer state, including bufferedTuples and bufferedBytes counters

## Dependencies
- Functions called/Symbols referenced:
  - [CopyMultiInsertInfo](CopyMultiInsertInfo.md) (struct type)
  - MAX_BUFFERED_TUPLES (constant defining maximum number of buffered tuples)
  - MAX_BUFFERED_BYTES (constant defining maximum buffered memory size)
- Called from (representative examples):
  - [CopyFrom](CopyFrom.md) (main COPY FROM function at src/backend/commands/copyfrom.c:1229)

## Notes and Other Information
- This is a static inline function optimized for performance within copyfrom.c
- The function implements an OR condition - either threshold being exceeded triggers a 'full' status
- Both thresholds are designed to prevent excessive memory usage and maintain reasonable batch sizes
- The dual-threshold approach handles cases where tuples vary significantly in size
- Return value of true indicates that buffers should be flushed before accepting more tuples
- The function is lightweight and designed to be called frequently during COPY operations
- MAX_BUFFERED_TUPLES and MAX_BUFFERED_BYTES are compile-time constants that define system-wide limits

## Simplified Source

```c
static inline bool CopyMultiInsertInfoIsFull(CopyMultiInsertInfo *miinfo) {
    if (miinfo->bufferedTuples >= MAX_BUFFERED_TUPLES ||
        miinfo->bufferedBytes >= MAX_BUFFERED_BYTES)
        return true;
    return false;
}
```