# offset_elem_desc

## Location
src/backend/access/rmgrdesc/rmgrdesc_utils.c: 44 - 49

## Overview
A callback function that formats OffsetNumber values as unsigned integers for use with the array_desc utility function.

## Definition


## Detailed Description
The  function is a specialized element description callback designed to work with the  utility function. It formats OffsetNumber values (which represent tuple positions within a page) as unsigned integers in the output buffer. This function follows the standard callback signature expected by  and is commonly used when describing arrays of offset numbers in WAL record descriptions.

The function takes a void pointer to an OffsetNumber, casts it appropriately, dereferences it, and formats it as an unsigned integer using .

## Parameters / Member Variables
- : StringInfo buffer where the formatted offset number will be appended
- : Pointer to the OffsetNumber value to be formatted (cast from void*)
- : Additional data parameter (unused in this implementation but required by callback signature)

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfo
  - OffsetNumber (data type)
- Called from (representative examples):
  - plan_elem_desc
  - heap2_desc
  - delvacuum_desc

## Notes and Other Information
- This is a callback function specifically designed for use with array_desc
- OffsetNumber is a PostgreSQL data type representing tuple positions within a page
- The function follows the standard element description callback signature
- The data parameter is not used but must be present to match the expected callback interface
- Commonly used in heap and btree WAL record descriptions to format arrays of tuple offsets