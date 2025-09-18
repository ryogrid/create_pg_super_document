# MXOffsetToMemberOffset

## Location
src/backend/access/transam/multixact.c: 205 - 214

## Overview
Calculates the byte offset within a page where the TransactionId of a specific MultiXact member is stored.

## Definition
```c
static inline int MXOffsetToMemberOffset(MultiXactOffset offset)
```

## Detailed Description
This function computes the exact location within a MultiXact member page where a specific member's TransactionId is stored. MultiXact member pages have a structured layout where each group contains flag bytes followed by the actual TransactionId values for the members in that group. The function combines multiple offset calculations to pinpoint the exact byte location of a member's TransactionId.

The calculation involves:
1. Determining the member's position within its group using modulo MULTIXACT_MEMBERS_PER_MEMBERGROUP
2. Finding the start of the member group using MXOffsetToFlagsOffset()
3. Adding the size of the flag bytes area (MULTIXACT_FLAGBYTES_PER_GROUP)
4. Adding the offset for the specific member based on its position and TransactionId size

## Parameters / Member Variables
- `offset`: A MultiXactOffset value that identifies a specific member within the MultiXact system

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactOffset (type)
  - MULTIXACT_MEMBERS_PER_MEMBERGROUP (constant)
  - MXOffsetToFlagsOffset (function)
  - MULTIXACT_FLAGBYTES_PER_GROUP (constant)
  - sizeof(TransactionId) (size calculation)
- Called from (representative examples):
  - RecordNewMultiXact
  - GetMultiXactIdMembers
  - TrimMultiXact

## Notes and Other Information
- This is a static inline function optimized for performance during MultiXact member access operations
- The function builds upon MXOffsetToFlagsOffset to provide the complete addressing scheme for MultiXact member pages
- The layout assumes that flag bytes come first in each group, followed immediately by the TransactionId array
- This function is essential for both reading existing MultiXact members and writing new ones during transaction processing
- The returned offset can be used directly with page access functions to read or write the TransactionId