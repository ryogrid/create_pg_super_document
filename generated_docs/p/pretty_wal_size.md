pretty_wal_size

## Overview
Calculates and formats a human-readable WAL (Write-Ahead Logging) size string with appropriate units (GB or MB) based on the number of WAL segments.

## Definition
static char *pretty_wal_size(int segment_count)

## Detailed Description
This utility function converts a segment count into a formatted string representing the total WAL size in a user-friendly format. It calculates the total size by multiplying the segment count by the WAL segment size in megabytes (wal_segment_size_mb). The function automatically chooses the most appropriate unit: if the size is evenly divisible by 1024, it formats the result in gigabytes (GB), otherwise it uses megabytes (MB).

The function allocates memory for the result string and uses snprintf to format the output with the calculated size and appropriate unit suffix. This is typically used during database initialization to display WAL configuration values in a readable format.

## Parameters / Member Variables
- segment_count: The number of WAL segments to calculate the total size for

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](pg_malloc.md)
- Called from (representative examples):
  - [setup_config](../s/setup_config.md) (used twice around lines 1338 and 1341)

## Notes and Other Information
- This is a static function within initdb.c, used specifically during database cluster initialization
- The function allocates exactly 14 bytes for the result string, which is sufficient for the largest possible values
- Uses wal_segment_size_mb global variable to determine the size of individual WAL segments
- Returns a dynamically allocated string that the caller is responsible for freeing
- Part of the WAL configuration setup process during initdb
- The choice between MB and GB units is purely for display readability

## Simplified Source

```c
static char *pretty_wal_size(int segment_count) {
    // Calculate total size in MB
    int sz = wal_segment_size_mb * segment_count;
    char *result = pg_malloc(14);

    // Format as GB if evenly divisible by 1024, otherwise MB
    if ((sz % 1024) == 0)
        snprintf(result, 14, "%dGB", sz / 1024);
    else
        snprintf(result, 14, "%dMB", sz);

    return result;
}
```