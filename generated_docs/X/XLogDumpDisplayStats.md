# XLogDumpDisplayStats

## Location
[src/bin/pg_waldump/pg_waldump.c:626-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L626-L755)

## Overview
A comprehensive statistics display function in pg_waldump that formats and prints detailed WAL record analysis statistics, showing counts and sizes by resource manager and optionally by individual record types.

## Definition

```c
static void
XLogDumpDisplayStats(XLogDumpConfig *config, XLogStats *stats)
```
## Detailed Description
XLogDumpDisplayStats generates a formatted statistical report of WAL records processed during pg_waldump analysis. The function first calculates totals across all resource managers, then displays statistics in a tabular format showing record counts, record sizes, full page image (FPI) sizes, and combined sizes with percentages. It supports two display modes: by resource manager only, or with detailed breakdown by individual record types within each resource manager. The output includes column headers, individual rows for each category, separator lines, and a summary total row with percentage breakdowns.

## Parameters / Member Variables
- `*config`: Configuration object containing display options and settings for the waldump analysis
- `*stats`: Statistics structure containing accumulated counts and sizes for WAL records processed
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid: Checks if LSN pointer is invalid
  - RmgrIdIsValid: Validates resource manager ID
  - [GetRmgrDesc](../G/GetRmgrDesc.md): Retrieves resource manager descriptor
  - [RmgrIdIsCustom](../R/RmgrIdIsCustom.md): Checks if resource manager is custom
  - [XLogDumpStatsRow](XLogDumpStatsRow.md): Displays individual statistic rows
  - printf: Standard output formatting
  - [psprintf](../p/psprintf.md): PostgreSQL string formatting function
  - LSN_FORMAT_ARGS: Macro for LSN formatting
  - INT64_MODIFIER: Platform-specific 64-bit integer format modifier
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function, only accessible within pg_waldump.c
- Early returns if no statistics have been computed (tracked by invalid endptr)
- Uses two-pass approach: first calculates totals, then displays individual rows with percentages
- Handles both aggregate (by resource manager) and detailed (by record type) display modes
- The detailed mode uses rm_identify callbacks to get human-readable record type names
- Column widths are carefully calculated for consistent tabular formatting
- Shows LSN range being analyzed in the header
- Final totals show percentages of record vs FPI data within the total combined size
- Skips custom resource managers that have zero records to reduce noise

## Simplified Source

```c
static void
XLogDumpDisplayStats(XLogDumpConfig *config, XLogStats *stats)
{
    // Return early if no stats computed yet
    if (XLogRecPtrIsInvalid(stats->endptr))
        return;

    uint64 total_count = 0, total_rec_len = 0, total_fpi_len = 0, total_len = 0;

    // Calculate totals across all resource managers
    for (int ri = 0; ri <= RM_MAX_ID; ri++) {
        if (!RmgrIdIsValid(ri))
            continue;
        total_count += stats->rmgr_stats[ri].count;
        total_rec_len += stats->rmgr_stats[ri].rec_len;
        total_fpi_len += stats->rmgr_stats[ri].fpi_len;
    }
    total_len = total_rec_len + total_fpi_len;

    // Print header and column headers
    printf("WAL statistics between %X/%X and %X/%X:\n",
           LSN_FORMAT_ARGS(stats->startptr), LSN_FORMAT_ARGS(stats->endptr));

    printf("%-27s %20s %8s %20s %8s %20s %8s %20s %8s\n",
           "Type", "N", "(%)", "Record size", "(%)", "FPI size", "(%)", "Combined size", "(%)");

    // Display stats for each resource manager
    for (int ri = 0; ri <= RM_MAX_ID; ri++) {
        if (!RmgrIdIsValid(ri))
            continue;

        const RmgrDescData *desc = GetRmgrDesc(ri);

        if (!config->stats_per_record) {
            // Show aggregate stats per resource manager
            uint64 count = stats->rmgr_stats[ri].count;
            uint64 rec_len = stats->rmgr_stats[ri].rec_len;
            uint64 fpi_len = stats->rmgr_stats[ri].fpi_len;
            uint64 tot_len = rec_len + fpi_len;

            if (RmgrIdIsCustom(ri) && count == 0)
                continue;

            XLogDumpStatsRow(desc->rm_name, count, total_count,
                           rec_len, total_rec_len, fpi_len, total_fpi_len,
                           tot_len, total_len);
        } else {
            // Show detailed stats per record type
            for (int rj = 0; rj < MAX_XLINFO_TYPES; rj++) {
                uint64 count = stats->record_stats[ri][rj].count;
                if (count == 0)
                    continue;

                uint64 rec_len = stats->record_stats[ri][rj].rec_len;
                uint64 fpi_len = stats->record_stats[ri][rj].fpi_len;
                uint64 tot_len = rec_len + fpi_len;

                const char *id = desc->rm_identify(rj << 4);
                if (id == NULL)
                    id = psprintf("UNKNOWN (%x)", rj << 4);

                XLogDumpStatsRow(psprintf("%s/%s", desc->rm_name, id),
                               count, total_count, rec_len, total_rec_len,
                               fpi_len, total_fpi_len, tot_len, total_len);
            }
        }
    }

    // Print totals row with percentages
    double rec_len_pct = (total_len != 0) ? 100 * (double) total_rec_len / total_len : 0;
    double fpi_len_pct = (total_len != 0) ? 100 * (double) total_fpi_len / total_len : 0;

    printf("%-27s %20" INT64_MODIFIER "u %-9s%20" INT64_MODIFIER "u %-9s"
           "%20" INT64_MODIFIER "u %-9s%20" INT64_MODIFIER "u %-6s\n",
           "Total", stats->count, "",
           total_rec_len, psprintf("[%.02f%%]", rec_len_pct),
           total_fpi_len, psprintf("[%.02f%%]", fpi_len_pct),
           total_len, "[100%]");
}
```