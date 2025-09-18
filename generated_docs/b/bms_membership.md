# bms_membership

## Location
[src/backend/nodes/bitmapset.c:781-814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L781-L814)

## Overview
Determines whether a bitmapset has zero, one, or multiple members by efficiently checking the membership state without counting all members.

## Definition


## Detailed Description
The  function efficiently determines the membership status of a bitmapset without performing a full member count. It returns one of three enumerated values:  for empty sets,  for sets with exactly one member, or  for sets with two or more members. This function is optimized for performance and is faster than using  when only the general membership category is needed.

The function iterates through the bitmap words and uses bitwise operations to detect the presence of multiple bits efficiently. It employs the  macro to quickly determine if a word contains more than one set bit.

## Parameters / Member Variables
- : Const pointer to the input Bitmapset to examine (can be NULL, which is treated as empty)

## Dependencies
- Functions called/Symbols referenced:
  - : Validates the bitmapset structure
  - : Macro to check if a bitmap word has multiple set bits
  - : Enumerated return type 
  - : Enum value for empty sets
  - : Enum value for single-member sets  
  - : Enum value for multi-member sets
  - : Type for bitmap word storage
- Called from (representative examples):
  - : Query planning for table sampling
  - : Foreign key join selectivity estimation
  - : Equivalence class processing
  - : Query planning with grouping operations
  - : Extended statistics dependency analysis

## Notes and Other Information
- Returns  for NULL input (treats NULL as empty set)
- More efficient than  when exact count is not required
- Commonly used in query optimization to make decisions based on set cardinality categories
- Essential for various PostgreSQL optimizer components including path planning, equivalence classes, and extended statistics