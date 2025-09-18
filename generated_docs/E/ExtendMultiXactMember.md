# ExtendMultiXactMember

## Location
[src/backend/access/transam/multixact.c:2577-2651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2577-L2651)

## Overview
Ensures that the MultiXactMember SLRU has sufficient space allocated for storing the members of a newly-allocated MultiXactId, handling multi-page scenarios and wraparound cases.

## Definition
static void ExtendMultiXactMember(MultiXactOffset offset, int nmembers)

## Detailed Description
This function extends the MultiXactMember SLRU buffer space to accommodate the storage requirements for a new MultiXactId's member list. It's designed to handle cases where members may span multiple pages of the members file. The function iterates through pages as needed, only performing initialization work when at the first entry of a new page.

The function includes sophisticated logic to handle wraparound scenarios, particularly for the last page of the last segment which has a different capacity than regular pages. It uses flag offset calculations to determine page boundaries and employs careful arithmetic to avoid overflow issues when dealing with maximum offset values.

Like ExtendMultiXactOffset, this function is called while holding MultiXactGenLock and is optimized for performance in the common case.

## Parameters / Member Variables
- `offset`: The starting MultiXactOffset where members will be stored
- `nmembers`: The number of members that need to be accommodated

## Dependencies
- Functions called/Symbols referenced:
  - [MXOffsetToFlagsOffset](../M/MXOffsetToFlagsOffset.md)
  - [MXOffsetToFlagsBitShift](../M/MXOffsetToFlagsBitShift.md)
  - [MXOffsetToMemberPage](../M/MXOffsetToMemberPage.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - LWLockAcquire
  - [ZeroMultiXactMemberPage](../Z/ZeroMultiXactMemberPage.md)
  - LWLockRelease
  - MAX_MEMBERS_IN_LAST_MEMBERS_PAGE
  - MaxMultiXactOffset
  - MULTIXACT_MEMBERS_PER_PAGE
- Called from (representative examples):
  - Internal MultiXact allocation routines

## Notes and Other Information
- Static function, internal to multixact.c
- Called while holding MultiXactGenLock for thread safety
- Handles multi-page member lists efficiently with a loop structure
- Includes special logic for the last page of the last segment
- Performs careful wraparound arithmetic to avoid overflow
- Only initializes pages at their first entry for optimal performance
- Uses SLRU bank locking for fine-grained concurrency control
- Creates XLOG entries for crash recovery consistency