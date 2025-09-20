# asyncQueuePageDiff

## Location
[src/backend/commands/async.c:466-475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L466-L475)

## Overview
A simple inline function that computes the arithmetic difference between two queue page numbers in PostgreSQL's asynchronous notification system.

## Definition

```c
static inline int64
asyncQueuePageDiff(int64 p, int64 q)
```
## Detailed Description
The  function performs a straightforward subtraction to calculate the difference between two queue page numbers. According to the code comments, this function previously accounted for wraparound scenarios, but the current implementation has been simplified to a basic arithmetic operation (p - q). This suggests that the queue page numbering system may have been redesigned to avoid wraparound issues, possibly by using a sufficiently large integer type (int64) that makes wraparound practically impossible.

## Parameters / Member Variables
- : The first queue page number (int64)
- : The second queue page number (int64)

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - NotificationHash
  - [SignalBackends](../S/SignalBackends.md)

## Notes and Other Information
- The function is declared as , making it internal to the async.c file and suitable for inlining by the compiler for performance
- The comment indicates this function previously handled wraparound logic, suggesting the queue page numbering system has evolved over time
- The use of int64 provides a very large range of page numbers, effectively eliminating wraparound concerns in practical scenarios
- This is a utility function used within the LISTEN/NOTIFY asynchronous messaging system