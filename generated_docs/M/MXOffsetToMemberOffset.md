# MXOffsetToMemberOffset

## Location
[src/backend/access/transam/multixact.c:205-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L205-L214)

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
  - [MXOffsetToFlagsOffset](MXOffsetToFlagsOffset.md) (function)
  - MULTIXACT_FLAGBYTES_PER_GROUP (constant)
  - sizeof(TransactionId) (size calculation)
- Called from (representative examples):
  - [RecordNewMultiXact](../R/RecordNewMultiXact.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [TrimMultiXact](../T/TrimMultiXact.md)

## Notes and Other Information
- This is a static inline function optimized for performance during MultiXact member access operations
- The function builds upon MXOffsetToFlagsOffset to provide the complete addressing scheme for MultiXact member pages
- The layout assumes that flag bytes come first in each group, followed immediately by the TransactionId array
- This function is essential for both reading existing MultiXact members and writing new ones during transaction processing
- The returned offset can be used directly with page access functions to read or write the TransactionId

## Simplified Source

```c
// Simplified version of MXOffsetToMemberOffset
static inline int
MXOffsetToMemberOffset(MultiXactOffset offset)
{
    // Find position of member within its group (0 to MULTIXACT_MEMBERS_PER_MEMBERGROUP-1)
    int member_in_group = offset % MULTIXACT_MEMBERS_PER_MEMBERGROUP;

    // Calculate byte offset: start of group + flag bytes + member position * TransactionId size
    return MXOffsetToFlagsOffset(offset) +
           MULTIXACT_FLAGBYTES_PER_GROUP +
           member_in_group * sizeof(TransactionId);
}
```

Key simplifications made:
- Added clear comments explaining each calculation step
- Clarified the member_in_group calculation purpose
- Explained the final offset calculation components
- Maintained original logic flow with improved readability