# initialize_counts

## Location
[src/backend/executor/nodeSetOp.c:80-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L80-L88)

## Overview
Initializes the tuple counting state for a new group of input values in SetOp operations, resetting both left and right input duplicate counters to zero.

## Definition

```c
static inline void
initialize_counts(SetOpStatePerGroup pergroup)
```
## Detailed Description
This function serves as a simple initialization routine for SetOp operations that need to track duplicate counts across input tuple groups. It resets the counting state stored in a SetOpStatePerGroup structure, which maintains separate counters for left-input and right-input duplicates within a group. This initialization is essential at the start of processing each new tuple group to ensure accurate duplicate counting for set operations like UNION, INTERSECT, and EXCEPT.

The function is implemented as a static inline function for performance, as it's a simple operation that gets called frequently during SetOp execution.

## Parameters / Member Variables
- : Pointer to a SetOpStatePerGroup structure that holds the per-group working state for counting duplicates

## Dependencies
- Functions called/Symbols referenced:
  - SetOpStatePerGroup (typedef structure)
- Called from (representative examples):
  - setop_retrieve_direct
  - setop_fill_hash_table

## Notes and Other Information
- This function is part of the SetOp executor node implementation in PostgreSQL
- Used in both SETOP_SORTED and SETOP_HASHED modes of operation
- The SetOpStatePerGroup structure contains two long integers: numLeft and numRight for tracking duplicate counts
- Essential for proper duplicate handling in SQL set operations