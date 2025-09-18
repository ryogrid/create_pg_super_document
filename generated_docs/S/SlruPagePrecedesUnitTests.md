# SlruPagePrecedesUnitTests

## Location
[src/backend/access/transam/slru.c:1694-1708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1694-L1708)

## Overview
Public unit testing function that validates SLRU PagePrecedes callback functions across multiple offset scenarios.

## Definition
```c
void SlruPagePrecedesUnitTests(SlruCtl ctl, int per_page)
```

## Detailed Description
SlruPagePrecedesUnitTests serves as a comprehensive test harness for validating SLRU PagePrecedes callback functions. It systematically tests the page precedence logic at three key positions within a page: the first entry, middle entry, and last entry. This ensures that the precedence logic works correctly regardless of where within a page the transaction ID falls.

The function assumes that:
- Every uint32 value >= FirstNormalTransactionId represents a valid key
- Each value occupies a contiguous, fixed-size region of SLRU bytes
- The SLRU structure supports random access by key

Note that this testing framework does not apply to all SLRU types - specifically MultiXactMemberCtl (which separates flags from XIDs) and NotifyCtl (which has variable-length entries and no random access) are excluded from this testing approach.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing the SLRU configuration and PagePrecedes callback to be tested
- `per_page`: int representing the number of transaction IDs that fit in one SLRU page

## Dependencies
- Functions called/Symbols referenced:
  - [SlruPagePrecedesTestOffset](SlruPagePrecedesTestOffset.md) (called three times with different offsets)
- Called from (representative examples):
  - [CLOGShmemInit](../C/CLOGShmemInit.md) (CLOG initialization)
  - CommitTsShmemInit (Commit timestamp initialization) 
  - [MultiXactShmemInit](../M/MultiXactShmemInit.md) (MultiXact initialization)
  - SUBTRANSShmemInit (Subtransaction initialization)
  - SerialInit (Serializable snapshot initialization)

## Notes and Other Information
- This is a public function used during PostgreSQL subsystem initialization
- Provides systematic validation of page precedence logic across different SLRU types
- Essential for ensuring transaction ID wrap-around handling works correctly
- Called during shared memory initialization for various transaction-related subsystems
- The three test positions (first, middle, last) provide comprehensive coverage of intra-page scenarios
- Part of PostgreSQL's built-in testing infrastructure for transaction management systems
- Does not apply to all SLRU types due to varying data layout assumptions