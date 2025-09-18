# SlruPagePrecedesTestOffset

## Location
src/backend/access/transam/slru.c: 1612 - 1693

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