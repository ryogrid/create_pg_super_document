# PageIsVerifiedExtended

## Location
[src/backend/storage/page/bufpage.c:88-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L88-L193)

## Overview
Validates that a page header and checksum appear correct when a page is read from disk, providing protection against corrupted data before processing page contents.

## Definition

```c
bool
PageIsVerifiedExtended(Page page, BlockNumber blkno, int flags)
```
## Detailed Description
PageIsVerifiedExtended performs comprehensive validation of a PostgreSQL page that has just been read from disk. The function serves as a first line of defense against data corruption by checking the page header structure, validating checksums (if enabled), and ensuring page boundaries are sensible before allowing the page into the buffer pool. It handles special cases like all-zero pages, which can legitimately exist due to system crashes during page extension. The function supports configurable error reporting through flags and can optionally ignore checksum failures in certain scenarios.

## Parameters / Member Variables
- `page`: Pointer to the page buffer to be verified
- `blkno`: Block number of the page for checksum calculation
- `flags`: Control flags for error reporting behavior (PIV_LOG_WARNING, PIV_REPORT_STAT)
## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast)
  - [PageIsNew](PageIsNew.md) (checks if page is all zeros)
  - [DataChecksumsEnabled](../D/DataChecksumsEnabled.md) (checks if checksums are enabled)
  - [pg_checksum_page](../p/pg_checksum_page.md) (calculates page checksum)
  - [pgstat_report_checksum_failure](../p/pgstat_report_checksum_failure.md) (reports checksum failures to stats)
  - PD_VALID_FLAG_BITS (valid page flag bits constant)
  - PIV_LOG_WARNING (flag for logging warnings)
  - PIV_REPORT_STAT (flag for reporting to statistics)
  - ERRCODE_DATA_CORRUPTED (error code constant)
- Called from (representative examples):
  - [RelationCopyStorage](../R/RelationCopyStorage.md) (storage copy operations)
  - [WaitReadBuffers](../W/WaitReadBuffers.md) (buffer manager read operations)
  - PageIsVerified (inline wrapper function)

## Notes and Other Information
- Allows zeroed pages as valid since they can occur after crashes during relation extension
- Checksum validation is only performed when DataChecksumsEnabled() returns true
- Header sanity checks include flag validation and boundary consistency (pd_lower <= pd_upper <= pd_special)
- All-zero page detection uses efficient word-aligned comparison
- The ignore_checksum_failure global variable can override checksum validation failures
- Returns true for valid pages (including all-zero pages), false for corrupted pages
- Error reporting is configurable through the flags parameter for different use cases

## Simplified Source

```c
bool
PageIsVerifiedExtended(Page page, BlockNumber blkno, int flags)
{
    PageHeader p = (PageHeader) page;
    bool checksum_failure = false;
    bool header_sane = false;
    bool all_zeroes = false;

    // Skip verification for new (empty) pages
    if (!PageIsNew(page)) {
        // Verify checksum if enabled
        if (DataChecksumsEnabled()) {
            uint16 checksum = pg_checksum_page((char *) page, blkno);
            if (checksum != p->pd_checksum)
                checksum_failure = true;
        }

        // Basic header sanity checks
        if ((p->pd_flags & ~PD_VALID_FLAG_BITS) == 0 &&
            p->pd_lower <= p->pd_upper &&
            p->pd_upper <= p->pd_special &&
            p->pd_special <= BLCKSZ &&
            p->pd_special == MAXALIGN(p->pd_special))
            header_sane = true;

        // Return early if page looks good
        if (header_sane && !checksum_failure)
            return true;
    }

    // Check if page is all zeros (valid after crashes)
    size_t *pagebytes = (size_t *) page;
    all_zeroes = true;
    for (int i = 0; i < (BLCKSZ / sizeof(size_t)); i++) {
        if (pagebytes[i] != 0) {
            all_zeroes = false;
            break;
        }
    }

    if (all_zeroes)
        return true;

    // Handle checksum failures with optional reporting
    if (checksum_failure) {
        if (flags & PIV_LOG_WARNING)
            ereport(WARNING, (errmsg("page verification failed")));

        if (flags & PIV_REPORT_STAT)
            pgstat_report_checksum_failure();

        // Allow override for checksum failures with good headers
        if (header_sane && ignore_checksum_failure)
            return true;
    }

    return false;
}
```