# redirect_elem_desc

## Location
[src/backend/access/rmgrdesc/rmgrdesc_utils.c:50-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/rmgrdesc_utils.c#L50-L57)

## Overview
A callback function that formats redirect mappings between offset numbers, displaying them in "source->target" format for WAL record descriptions.

## Definition

```c
void
redirect_elem_desc(StringInfo buf, void *offset, void *data)
```
## Detailed Description
The  function is a specialized element description callback designed to work with the  utility function. It formats pairs of OffsetNumber values to show redirection mappings in the format "source->target". This function is specifically used for describing heap page redirections in WAL records, where tuples may be redirected from one offset to another during operations like HOT (Heap-Only Tuples) updates.

The function expects the input to be a pointer to an array of two OffsetNumber values, where the first element is the source offset and the second is the target offset.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted redirect mapping will be appended
- `*offset`: Pointer to an array of two OffsetNumber values representing the redirect mapping
- `*data`: Additional data parameter (unused in this implementation but required by callback signature)
## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfo](../a/appendStringInfo.md)
  - OffsetNumber (data type)
- Called from (representative examples):
  - [heap2_desc](../h/heap2_desc.md)

## Notes and Other Information
- This is a callback function specifically designed for use with array_desc
- Used primarily in heap WAL record descriptions for HEAP2_CLEAN operations
- The input is expected to be an array of exactly two OffsetNumber values
- The arrow notation ("->" indicates redirection from source to target offset
- Part of PostgreSQL's HOT (Heap-Only Tuples) infrastructure for tracking tuple redirections
- The data parameter is unused but required to match the standard callback interface

## Simplified Source

```c
void redirect_elem_desc(StringInfo buf, void *offset, void *data) {
    // Cast to array of two OffsetNumbers
    OffsetNumber *new_offset = (OffsetNumber *) offset;

    // Format as "source->target" redirection
    appendStringInfo(buf, "%u->%u", new_offset[0], new_offset[1]);
}
```