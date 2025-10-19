# XLogDumpStatsRow

## Location
[src/bin/pg_waldump/pg_waldump.c:585-625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L585-L625)

## Overview
A utility function in pg_waldump that displays a single row of statistical information for WAL record counts and sizes, typically for a specific resource manager (rmgr) or record type.

## Definition

```c
static void
XLogDumpStatsRow(const char *name,
				 uint64 n, uint64 total_count,
				 uint64 rec_len, uint64 total_rec_len,
				 uint64 fpi_len, uint64 total_fpi_len,
				 uint64 tot_len, uint64 total_len)
```
## Detailed Description
XLogDumpStatsRow formats and prints a single statistical row showing record counts and data sizes for WAL analysis. It calculates percentage values for each metric relative to the total and displays them in a formatted table row. The function handles division by zero cases by setting percentages to 0 when totals are zero. The output includes the name/identifier, record count with percentage, record length with percentage, full page image (FPI) length with percentage, and total length with percentage.

## Parameters / Member Variables
- `*name`: String identifier for the row (typically rmgr name or record type)
- `n`: Number of records for this category
- `total_count`: Total number of records across all categories
- `rec_len`: Total record length (bytes) for this category
- `total_rec_len`: Total record length across all categories
- `fpi_len`: Total full page image length (bytes) for this category
- `total_fpi_len`: Total FPI length across all categories
- `tot_len`: Total combined length (bytes) for this category
- `total_len`: Total combined length across all categories
## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
  - INT64_MODIFIER (PostgreSQL macro for platform-specific 64-bit integer formatting)
- Called from (representative examples):
  - [XLogDumpDisplayStats](XLogDumpDisplayStats.md)

## Notes and Other Information
- This is a static function, only accessible within the pg_waldump.c file
- Uses careful division-by-zero protection when calculating percentages
- Formats output in a consistent tabular format with fixed-width columns
- Part of the pg_waldump utility for analyzing PostgreSQL Write-Ahead Log files
- The formatted output shows both absolute values and percentages for easy comparison across categories

## Simplified Source

```c
static void
XLogDumpStatsRow(const char *name,
                 uint64 n, uint64 total_count,
                 uint64 rec_len, uint64 total_rec_len,
                 uint64 fpi_len, uint64 total_fpi_len,
                 uint64 tot_len, uint64 total_len)
{
    // Calculate percentages (safely handle division by zero)
    double n_pct = (total_count != 0) ? 100 * (double) n / total_count : 0;
    double rec_len_pct = (total_rec_len != 0) ? 100 * (double) rec_len / total_rec_len : 0;
    double fpi_len_pct = (total_fpi_len != 0) ? 100 * (double) fpi_len / total_fpi_len : 0;
    double tot_len_pct = (total_len != 0) ? 100 * (double) tot_len / total_len : 0;

    // Print formatted statistics row
    printf("%-27s "
           "%20" INT64_MODIFIER "u (%6.02f) "
           "%20" INT64_MODIFIER "u (%6.02f) "
           "%20" INT64_MODIFIER "u (%6.02f) "
           "%20" INT64_MODIFIER "u (%6.02f)\n",
           name, n, n_pct, rec_len, rec_len_pct, fpi_len, fpi_len_pct,
           tot_len, tot_len_pct);
}
```