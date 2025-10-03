# report_invalid_page

## Location
[src/backend/access/transam/xlogutils.c:86-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L86-L101)

## Overview
Reports an invalid page reference by logging an appropriate error message, differentiating between uninitialized pages and non-existent pages.

## Definition

```c
static void
report_invalid_page(int elevel, RelFileLocator locator, ForkNumber forkno,
					BlockNumber blkno, bool present)
```
## Detailed Description
The  function is a static utility function in the PostgreSQL WAL (Write-Ahead Log) utilities that generates descriptive error messages when invalid pages are encountered. It constructs a human-readable path for the relation and logs either "uninitialized" or "does not exist" messages depending on the  parameter. This function helps administrators and developers understand the nature of page-related issues during recovery or normal operations.

## Parameters / Member Variables
- `elevel`: Log level for the error message (e.g., ERROR, WARNING, LOG)
- `locator`: RelFileLocator structure identifying the relation (tablespace, database, relation OID)
- `forkno`: Fork number indicating which fork of the relation (main, FSM, VM, etc.)
- `blkno`: Block number of the invalid page
- `present`: Boolean flag - true if page exists but is uninitialized, false if page doesn't exist
## Dependencies
- Functions called/Symbols referenced:
  - relpathperm
  - elog
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [log_invalid_page](../l/log_invalid_page.md)
  - [XLogCheckInvalidPages](../X/XLogCheckInvalidPages.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the xlogutils.c compilation unit
- Memory allocated by relpathperm is properly freed with pfree to prevent memory leaks
- The function provides clear distinction between two types of invalid page conditions to aid in diagnosis
- Used primarily during WAL replay and recovery operations when PostgreSQL encounters problematic pages

## Simplified Source
```c
static void report_invalid_page(int elevel, RelFileLocator locator, ForkNumber forkno,
                               BlockNumber blkno, bool present)
{
    char *path = relpathperm(locator, forkno);

    if (present) {
        elog(elevel, "page %u of relation %s is uninitialized", blkno, path);
    } else {
        elog(elevel, "page %u of relation %s does not exist", blkno, path);
    }

    pfree(path);
}
```