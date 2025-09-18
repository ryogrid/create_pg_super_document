# range_get_flags

## Location
[src/backend/utils/adt/rangetypes.c:1923-1936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1923-L1936)

## Overview
A lightweight utility function that extracts only the flags byte from a serialized RangeType value without full deserialization.

## Definition


## Detailed Description
This function provides an efficient way to access just the flags information from a range object without the overhead of full deserialization. It directly reads the flags byte from the last byte of the range object's binary representation. This is particularly useful for functions that only need to check properties like emptiness, bound inclusivity, or infinity without needing the actual bound values.

## Parameters / Member Variables
- : Serialized range object from which to extract flags

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE
- Called from (representative examples):
  - [range_out](range_out.md)
  - [range_send](range_send.md)
  - [range_empty](range_empty.md)
  - [range_lower_inc](range_lower_inc.md)
  - [range_upper_inc](range_upper_inc.md)
  - [range_lower_inf](range_lower_inf.md)
  - [range_upper_inf](range_upper_inf.md)
  - [hash_range](../h/hash_range.md)
  - RangeIsEmpty
  - RangeIsOrContainsEmpty

## Notes and Other Information
- Much more efficient than range_deserialize when only flags are needed
- The flags byte encodes information about emptiness, bound inclusivity, and infinity
- Commonly used by accessor functions that return specific range properties
- The flags are stored at the very end of the range object's binary representation
- Used extensively in range property checking and hash functions