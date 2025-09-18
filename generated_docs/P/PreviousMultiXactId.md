# PreviousMultiXactId

## Location
src/backend/access/transam/multixact.c: 220 - 230

## Overview
Returns the MultiXact ID that immediately precedes the given MultiXact ID, handling wraparound at the boundary between MaxMultiXactId and FirstMultiXactId.

## Definition
```c
static inline MultiXactId PreviousMultiXactId(MultiXactId multi)
```

## Detailed Description
This function implements the predecessor operation for MultiXact IDs with wraparound semantics. MultiXact IDs form a circular sequence that wraps around from MaxMultiXactId back to FirstMultiXactId. When given a MultiXact ID, this function returns the previous ID in the sequence, properly handling the wraparound case.

The logic is straightforward:
- If the input is FirstMultiXactId (the minimum valid ID), the previous ID is MaxMultiXactId (wraparound)
- For all other cases, simply subtract 1 from the input ID

This wraparound behavior is essential for PostgreSQL's transaction management system, which uses a circular numbering scheme to handle the finite range of MultiXact IDs efficiently.

## Parameters / Member Variables
- `multi`: The MultiXactId for which to find the preceding ID

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactId (type)
  - FirstMultiXactId (constant)
  - MaxMultiXactId (constant)
- Called from (representative examples):
  - [PerformOffsetsTruncation](PerformOffsetsTruncation.md)

## Notes and Other Information
- This is a static inline function for optimal performance during frequent MultiXact ID operations
- The wraparound semantics ensure that MultiXact ID arithmetic works correctly across the boundary conditions
- This function is part of the MultiXact ID management infrastructure that supports PostgreSQL's MVCC (Multi-Version Concurrency Control) system
- The function assumes that the input MultiXact ID is valid; no validation is performed
- Similar to transaction ID wraparound handling, this prevents issues when MultiXact IDs reach their maximum value and need to restart from the minimum