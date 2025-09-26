# pg_unicode_range

## Location
src/include/common/unicode_category_table.h: 32 - 37

## Overview
A structure that represents a contiguous range of Unicode codepoints, used for efficient storage and binary search of Unicode character properties in PostgreSQL's Unicode handling system.

## Definition

```c
typedef struct
{
	uint8		category;
	uint8		properties;
} pg_unicode_properties;
```
## Detailed Description
The  structure is a fundamental building block in PostgreSQL's Unicode character classification system. It defines a range of Unicode codepoints from  to 
wtmp begins Sun Aug 20 19:22:10 2023 (inclusive), allowing efficient representation of contiguous blocks of Unicode characters that share common properties.

This structure is extensively used to create lookup tables for various Unicode character properties such as alphabetic characters, hex digits, and other character categories. Rather than storing individual codepoints, ranges allow for compact representation of the Unicode standard's character classifications.

The structure is designed to work with PostgreSQL's binary search algorithms, particularly the  function, which performs efficient lookups to determine if a given Unicode codepoint falls within any of the ranges in a table.

## Parameters / Member Variables
- : The first Unicode codepoint in the range (inclusive, 32-bit unsigned integer)
- 
wtmp begins Sun Aug 20 19:22:10 2023: The last Unicode codepoint in the range (inclusive, 32-bit unsigned integer)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure definition)
- Called from (representative examples):
  -  (used as array element type for binary search)
  - Various Unicode property tables (e.g., )

## Notes and Other Information
- Used extensively in  to define tables for different Unicode character properties
- The structure supports the full Unicode range (up to 0x10FFFF) through 32-bit integers
- Ranges are typically stored in sorted arrays to enable efficient binary search operations
- This design allows PostgreSQL to handle Unicode character classification efficiently without storing every individual codepoint
- The structure is used in conjunction with other Unicode-related structures like  and 