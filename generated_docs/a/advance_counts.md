# advance_counts

## Location
[src/backend/executor/nodeSetOp.c:89-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L89-L101)

## Overview
Increments the appropriate tuple counter (left or right input) based on a flag value, tracking duplicate counts for SetOp operations.

## Definition

```c
static inline void
advance_counts(SetOpStatePerGroup pergroup, int flag)
```
## Detailed Description
This function updates the duplicate counting state for a tuple group in SetOp operations by incrementing either the left-input counter (numLeft) or right-input counter (numRight) based on the flag parameter. It serves as a core counting mechanism for PostgreSQL's set operations (UNION, INTERSECT, EXCEPT) that need to track how many duplicate tuples come from each input relation.

The function uses a simple conditional logic where a non-zero flag indicates a right-input tuple, while a zero flag indicates a left-input tuple. This binary classification allows the SetOp executor to maintain separate counts for tuples originating from different input sources.

## Parameters / Member Variables
- : Pointer to a SetOpStatePerGroup structure that maintains the per-group working state for counting duplicates
- : Integer flag indicating the input source (0 for left input, non-zero for right input)

## Dependencies
- Functions called/Symbols referenced:
  - [SetOpStatePerGroup](../S/SetOpStatePerGroup.md) (typedef structure)
- Called from (representative examples):
  - [setop_retrieve_direct](../s/setop_retrieve_direct.md) (multiple locations)
  - [setop_fill_hash_table](../s/setop_fill_hash_table.md) (multiple locations)

## Notes and Other Information
- Implemented as a static inline function for optimal performance during frequent calls
- Used in both SETOP_SORTED and SETOP_HASHED execution modes
- The flag parameter is typically derived from tuple metadata indicating which input relation produced the tuple
- Essential for implementing SQL set operation semantics where duplicate handling depends on input source

## Simplified Source

```c
static inline void
advance_counts(SetOpStatePerGroup pergroup, int flag)
{
    // Increment appropriate counter based on input source
    if (flag)
        pergroup->numRight++;  // Right input tuple
    else
        pergroup->numLeft++;   // Left input tuple
}
```