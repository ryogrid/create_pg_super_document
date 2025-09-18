# MXOffsetToFlagsBitShift

## Location
src/backend/access/transam/multixact.c: 195 - 204

## Overview
Calculates the bit shift position within a flag word for a specific MultiXact member, used to access the correct bits that store the member's status flags.

## Definition
```c
static inline int MXOffsetToFlagsBitShift(MultiXactOffset offset)
```

## Detailed Description
This function determines the bit shift amount needed to access the correct bits within a flag word for a specific MultiXact member. In PostgreSQL's MultiXact system, multiple members are packed into groups, and each member's flags are stored in specific bit positions within a shared flag word. The function calculates which bit position corresponds to a given member offset.

The calculation process:
1. Determines the member's position within its group using modulo MULTIXACT_MEMBERS_PER_MEMBERGROUP
2. Multiplies by MXACT_MEMBER_BITS_PER_XACT to get the bit shift amount, since each member uses a fixed number of bits for its flags

## Parameters / Member Variables
- `offset`: A MultiXactOffset value identifying a specific member within the MultiXact system

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactOffset (type)
  - MULTIXACT_MEMBERS_PER_MEMBERGROUP (constant)
  - MXACT_MEMBER_BITS_PER_XACT (constant)
- Called from (representative examples):
  - RecordNewMultiXact
  - GetMultiXactIdMembers
  - ExtendMultiXactMember

## Notes and Other Information
- This is a static inline function optimized for frequent use during MultiXact flag manipulation
- Works in conjunction with MXOffsetToFlagsOffset to provide complete addressing within MultiXact member pages
- The bit shift value is used with bitwise operations to extract or modify specific member flags
- Each member uses MXACT_MEMBER_BITS_PER_XACT bits to store its flags, allowing multiple members to share a single flag word efficiently