# PerformOffsetsTruncation

## Location
[src/backend/access/transam/multixact.c:3069-3093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3069-L3093)

## Overview
PerformOffsetsTruncation deletes MultiXact offset segments in a specified range using the SimpleLruTruncate mechanism with careful handling of edge cases.

## Definition
static void PerformOffsetsTruncation(MultiXactId oldestMulti, MultiXactId newOldestMulti)

## Detailed Description
This function performs truncation of MultiXact offset segments by leveraging the standard SimpleLruTruncate function. Unlike member truncation, offset truncation can use the simpler SLRU truncation mechanism because offsets have more predictable usage patterns.

The function includes a critical optimization where it steps back one MultiXact ID before performing the truncation. This prevents edge case issues where the cutoff page hasn't been created yet, specifically when oldestMulti would be the first item on a page and equals nextMulti. Without this adjustment, SimpleLruTruncate's wraparound detection could be incorrectly triggered.

## Parameters / Member Variables
- `oldestMulti`: The oldest MultiXact ID to be removed from the offset segments
- `newOldestMulti`: The new oldest MultiXact ID that should remain (truncation boundary)

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruTruncate](../S/SimpleLruTruncate.md)
  - MultiXactOffsetCtl
  - [MultiXactIdToOffsetPage](../M/MultiXactIdToOffsetPage.md)
  - [PreviousMultiXactId](PreviousMultiXactId.md)
- Called from (representative examples):
  - [TruncateMultiXact](../T/TruncateMultiXact.md)
  - [multixact_redo](../m/multixact_redo.md)

## Notes and Other Information
- Much simpler than member truncation due to predictable offset data patterns
- Uses PreviousMultiXactId() to avoid edge case wraparound detection issues
- Works directly with the standard SLRU truncation mechanism
- Part of the MultiXact maintenance system that manages transaction offset mappings
- The step-back logic is crucial for preventing false wraparound detection in SimpleLruTruncate