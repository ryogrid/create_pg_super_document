# SlruPagePrecedesTestOffset

## Location
[src/backend/access/transam/slru.c:1612-1693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1612-L1693)

## Overview
Internal test function that validates SLRU page precedence logic and segment deletion safety for transaction ID wrap-around scenarios.

## Definition
```c
static void SlruPagePrecedesTestOffset(SlruCtl ctl, int per_page, uint32 offset)
```

## Detailed Description
SlruPagePrecedesTestOffset is a comprehensive testing function that validates the correctness of SLRU page precedence logic, particularly focusing on PostgreSQL's transaction ID wrap-around handling. The function performs extensive assertions to ensure that:

1. **XID Precedence Logic**: Tests transaction ID comparison with pairs at "opposite ends" of the XID space, where each ID appears to precede the other due to wrap-around semantics (RFC 1982).

2. **Page Precedence Validation**: Verifies that the SLRU's PagePrecedes callback function correctly handles page-level comparisons, including edge cases around wrap boundaries.

3. **Segment Deletion Safety**: Tests two critical scenarios where segments must not be deleted:
   - When the newest XID is in the last page of the second segment
   - When the newest XID is in the first page of the second segment

The function uses mathematical relationships between transaction IDs and page numbers to create test scenarios that exercise boundary conditions in the SLRU system.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing SLRU control information and PagePrecedes callback
- `per_page`: int representing the number of transaction IDs per SLRU page
- `offset`: uint32 offset within the page to avoid non-normal XIDs

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (for XID comparison testing)
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md) (for XID relationship testing)
  - ctl->PagePrecedes (callback function being tested)
  - [SlruMayDeleteSegment](SlruMayDeleteSegment.md) (segment deletion safety validation)
  - SLRU_PAGES_PER_SEGMENT (segment size constant)
  - Assert (for validation assertions)
- Called from (representative examples):
  - [SlruPagePrecedesUnitTests](SlruPagePrecedesUnitTests.md) (multiple times with different offsets)

## Notes and Other Information
- This is a static (internal) test function used for SLRU validation
- Extensively uses assertions to validate complex wrap-around scenarios
- Critical for ensuring data safety during SLRU truncation operations
- Tests the RFC 1982 circular sequence space semantics used by PostgreSQL
- Validates that segments containing active transaction data are never deleted
- The function is designed to catch regressions in SLRU page ordering logic
- Part of PostgreSQL's internal testing infrastructure for transaction management

## Simplified Source

```c
// Simplified version of SlruPagePrecedesTestOffset
static void SlruPagePrecedesTestOffset(SlruCtl ctl, int per_page, uint32 offset)
{
    TransactionId lhs, rhs;
    int64 newestPage, oldestPage;
    TransactionId newestXact, oldestXact;

    // Test XID precedence at wrap-around boundary
    // Create pair at "opposite ends" of XID space where each precedes the other
    lhs = per_page + offset;        // skip first page for normal XIDs
    rhs = lhs + (1U << 31);        // exactly half the XID space away

    // Validate XID precedence behavior at wrap boundary
    Assert(TransactionIdPrecedes(lhs, rhs));
    Assert(TransactionIdPrecedes(rhs, lhs));    // both precede each other
    Assert(!TransactionIdPrecedes(lhs - 1, rhs));
    Assert(!TransactionIdFollowsOrEquals(lhs, rhs));

    // Test page precedence logic with various page relationships
    Assert(!ctl->PagePrecedes(lhs / per_page, rhs / per_page));
    Assert(ctl->PagePrecedes(rhs / per_page, (lhs - 3 * per_page) / per_page));
    Assert(ctl->PagePrecedes((lhs + 3 * per_page) / per_page, rhs / per_page));

    // Test scenario 1: Newest XID in LAST page of second segment
    newestPage = 2 * SLRU_PAGES_PER_SEGMENT - 1;
    newestXact = newestPage * per_page + offset;
    oldestXact = newestXact + 1 - (1U << 31);   // wrap around
    oldestPage = oldestXact / per_page;

    // Ensure segment containing newest XID is not deleted
    Assert(!SlruMayDeleteSegment(ctl,
                                newestPage - (newestPage % SLRU_PAGES_PER_SEGMENT),
                                oldestPage));

    // Test scenario 2: Newest XID in FIRST page of second segment
    newestPage = SLRU_PAGES_PER_SEGMENT;
    newestXact = newestPage * per_page + offset;
    oldestXact = newestXact + 1 - (1U << 31);   // wrap around
    oldestPage = oldestXact / per_page;

    // Ensure segment containing newest XID is not deleted
    Assert(!SlruMayDeleteSegment(ctl,
                                newestPage - (newestPage % SLRU_PAGES_PER_SEGMENT),
                                oldestPage));
}
```

Key simplifications made:
- Consolidated multiple similar Assert statements into representative examples
- Added comments explaining the wrap-around XID logic
- Simplified variable calculations by combining operations
- Removed redundant edge case assertions while preserving core logic
- Clarified the two main test scenarios with descriptive comments
- Maintained the essential algorithm for testing page precedence and segment safety