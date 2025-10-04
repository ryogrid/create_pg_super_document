# intset_add_member

## Location
[src/backend/lib/integerset.c:370-395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L370-L395)

## Overview
Adds a 64-bit integer value to an IntegerSet data structure, maintaining the requirement that values must be added in strictly ascending order.

## Definition

```c
void
intset_add_member(IntegerSet *intset, uint64 x)
```
## Detailed Description
The  function is responsible for adding new integer values to a PostgreSQL IntegerSet. It implements a buffered insertion strategy where new values are first stored in a temporary buffer before being flushed to the compressed B-tree structure. This approach optimizes performance by batching insertions and reducing the overhead of immediate compression.

The function enforces strict ordering requirements - values must be added in ascending order, and no value can be added that is less than or equal to the previously highest value in the set. It also prevents modifications during active iteration to maintain data integrity.

When the internal buffer reaches its capacity (MAX_BUFFERED_VALUES), the function automatically triggers a flush operation to compress and store the buffered values into the main B-tree structure.

## Parameters
- : Pointer to the IntegerSet structure to which the value will be added
- : The 64-bit unsigned integer value to add to the set

## Dependencies
- Functions called/Symbols referenced:
  - [intset_flush_buffered_values](intset_flush_buffered_values.md)
  - MAX_BUFFERED_VALUES
  - [IntegerSet](../I/IntegerSet.md)
- Called from (representative examples):
  - [gistvacuumpage](../g/gistvacuumpage.md)
  - [test_pattern](../t/test_pattern.md)
  - [test_single_value](../t/test_single_value.md)
  - [test_single_value_and_filler](../t/test_single_value_and_filler.md)

## Notes and Other Information
- Values must be added in strictly ascending order; adding out-of-order values will result in an ERROR
- Cannot add values while iteration is active on the set
- Automatically manages buffer flushing when the buffer capacity is reached
- Updates the set's metadata including entry count and highest value
- Part of PostgreSQL's compressed integer set implementation used for efficient storage of large sets of integers

## Simplified Source

```c
void intset_add_member(IntegerSet *intset, uint64 x) {
    // Ensure no iteration is active
    if (intset->iter_active)
        elog(ERROR, "cannot add new values to integer set while iteration is in progress");

    // Enforce ascending order requirement
    if (x <= intset->highest_value && intset->num_entries > 0)
        elog(ERROR, "cannot add value to integer set out of order");

    // Flush buffer if full
    if (intset->num_buffered_values >= MAX_BUFFERED_VALUES) {
        intset_flush_buffered_values(intset);
        Assert(intset->num_buffered_values < MAX_BUFFERED_VALUES);
    }

    // Add value to buffer and update metadata
    intset->buffered_values[intset->num_buffered_values] = x;
    intset->num_buffered_values++;
    intset->num_entries++;
    intset->highest_value = x;
}
```