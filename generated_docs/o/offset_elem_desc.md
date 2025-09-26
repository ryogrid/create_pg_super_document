# offset_elem_desc

## Location
[src/backend/access/rmgrdesc/rmgrdesc_utils.c:44-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/rmgrdesc_utils.c#L44-L49)

## Overview
A callback function that formats OffsetNumber values as unsigned integers for use with the array_desc utility function.

## Definition

```c
void
offset_elem_desc(StringInfo buf, void *offset, void *data)
```
## Detailed Description
The  function is a specialized element description callback designed to work with the  utility function. It formats OffsetNumber values (which represent tuple positions within a page) as unsigned integers in the output buffer. This function follows the standard callback signature expected by  and is commonly used when describing arrays of offset numbers in WAL record descriptions.

The function takes a void pointer to an OffsetNumber, casts it appropriately, dereferences it, and formats it as an unsigned integer using .

## Parameters / Member Variables
- : StringInfo buffer where the formatted offset number will be appended
- : Pointer to the OffsetNumber value to be formatted (cast from void*)
- : Additional data parameter (unused in this implementation but required by callback signature)

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfo](../a/appendStringInfo.md)
  - OffsetNumber (data type)
- Called from (representative examples):
  - [plan_elem_desc](../p/plan_elem_desc.md)
  - [heap2_desc](../h/heap2_desc.md)
  - [delvacuum_desc](../d/delvacuum_desc.md)

## Notes and Other Information
- This is a callback function specifically designed for use with array_desc
- OffsetNumber is a PostgreSQL data type representing tuple positions within a page
- The function follows the standard element description callback signature
- The data parameter is not used but must be present to match the expected callback interface
- Commonly used in heap and btree WAL record descriptions to format arrays of tuple offsets