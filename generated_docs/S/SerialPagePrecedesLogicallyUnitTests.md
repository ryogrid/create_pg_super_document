# SerialPagePrecedesLogicallyUnitTests

## Location
[src/backend/storage/lmgr/predicate.c:747-805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L747-L805)

## Overview
Provides unit tests for the SerialPagePrecedesLogically function to verify correct behavior in edge cases involving transaction ID wraparound scenarios.

## Definition
```c
static void SerialPagePrecedesLogicallyUnitTests(void)
```

## Detailed Description
This function contains comprehensive unit tests that validate the SerialPagePrecedesLogically function in extreme scenarios involving PostgreSQL's circular transaction ID space. It tests two critical edge cases:

1. **Scenario 1**: Tests when the SLRU headPage pertains to recently assigned XIDs (~last 1000), while oldestXact finished after ~2 billion XIDs elapsed. The function must return false to prevent SerialAdd() from zeroing pages that may contain entries for other old, recently-finished transactions.

2. **Scenario 2**: Tests when headPage pertains to oldestXact and we're summarizing an XID near newestXact. The function should return true to allow SerialAdd() to create the target page. The comments note that the current implementation has a known defect in this case, but it's considered negligible due to the extreme rarity of the scenario.

The tests simulate burning approximately 2 billion transaction IDs in single-user mode, which is considered a negligible possibility in real-world usage.

## Parameters / Member Variables
This function takes no parameters but uses several local variables:
- `per_page`: Number of entries per serial page (SERIAL_ENTRIESPERPAGE)
- `offset`: Half-page offset for calculations
- `newestPage`, `oldestPage`, `headPage`, `targetPage`: Page numbers for test scenarios
- `newestXact`, `oldestXact`: Transaction IDs for test scenarios

## Dependencies
- Functions called/Symbols referenced:
  - `[SerialPagePrecedesLogically](SerialPagePrecedesLogically.md)`
  - `SERIAL_ENTRIESPERPAGE`
  - `SLRU_PAGES_PER_SEGMENT`
- Called from (representative examples):
  - `[SerialInit](SerialInit.md)`

## Notes and Other Information
- Only compiled and executed when USE_ASSERT_CHECKING is enabled
- Tests extreme edge cases that require burning ~2B XIDs in single-user mode
- Documents a known implementation defect that affects just one page in an extremely rare scenario
- The defect's consequence would be mild: a new transaction failing in SimpleLruReadPage()
- Contains disabled assertion (under #if 0) that demonstrates the known defect