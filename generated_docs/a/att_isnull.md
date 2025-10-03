# att_isnull

## Location
[src/include/access/tupmacs.h:26-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tupmacs.h#L26-L45)

## Overview
Checks a tuple's null bitmap to determine whether a specific attribute is null by examining the corresponding bit in the null bitmap array.

## Definition

```c
static inline bool
att_isnull(int ATT, const bits8 *BITS)
```
## Detailed Description
The att_isnull function is a low-level utility that examines a tuple's null bitmap to determine if a specific attribute is null. PostgreSQL uses a bitmap representation where each bit corresponds to an attribute in the tuple - a 0 bit indicates null, while a 1 bit indicates non-null. The function uses bit manipulation to efficiently check the appropriate bit by calculating the byte index (ATT >> 3) and the bit position within that byte (ATT & 0x07).

## Parameters / Member Variables
- `ATT`: The zero-based attribute number to check for nullness
- `*BITS`: Pointer to the null bitmap array (bits8 array) for the tuple
## Dependencies
- Functions called/Symbols referenced:
  - bits8 (type definition)
  - FRONTEND (conditional compilation flag)
- Called from (representative examples):
  - [brin_deconstruct_tuple](../b/brin_deconstruct_tuple.md)
  - [heap_attisnull](../h/heap_attisnull.md)
  - [nocachegetattr](../n/nocachegetattr.md)
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)
  - [fastgetattr](../f/fastgetattr.md)
  - [index_getattr](../i/index_getattr.md)

## Notes and Other Information
- This is an inline function for performance optimization since it's called frequently during tuple processing
- The bit manipulation logic (ATT >> 3) divides by 8 to find the byte index, while (ATT & 0x07) finds the bit position within that byte
- The function returns true if the attribute is null (bit is 0) and false if non-null (bit is 1)
- This function is fundamental to PostgreSQL's tuple representation and is used throughout the system for null checking
- The function is defined in tupmacs.h, indicating its role as a core tuple manipulation macro/function

## Simplified Source

```c
static inline bool att_isnull(int ATT, const bits8 *BITS) {
    // Calculate byte index: ATT >> 3 divides by 8
    // Calculate bit position: ATT & 0x07 gives remainder (0-7)
    // Check if the bit is 0 (null) - invert because 0=null, 1=non-null
    return !(BITS[ATT >> 3] & (1 << (ATT & 0x07)));
}
```