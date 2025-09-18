# bbsink_throttle

## Location
src/backend/backup/basebackup_throttle.c: 23 - 39

## Overview
A struct that implements a throttling basebackup sink, controlling the transfer rate of backup data by sleeping when the data transfer exceeds the configured maximum rate.

## Definition
```c
typedef struct bbsink_throttle
{
    /* Common information for all types of sink. */
    bbsink      base;

    /* The actual number of bytes, transfer of which may cause sleep. */
    uint64      throttling_sample;

    /* Amount of data already transferred but not yet throttled.  */
    int64       throttling_counter;

    /* The minimum time required to transfer throttling_sample bytes. */
    TimeOffset  elapsed_min_unit;

    /* The last check of the transfer rate. */
    TimestampTz throttled_last;
} bbsink_throttle;
```

## Detailed Description
The `bbsink_throttle` struct is a specialized basebackup sink that implements rate limiting for PostgreSQL base backup operations. It acts as a filter in the basebackup sink chain, monitoring the amount of data being transferred and introducing delays when the transfer rate exceeds the specified maximum rate.

The throttling mechanism works by dividing the maximum allowed transfer rate into smaller samples (based on `THROTTLING_FREQUENCY` constant set to 8). When the accumulated transferred data reaches the sample threshold, the struct calculates how much time should have elapsed for this amount of data at the target rate, and sleeps if the actual transfer was too fast.

The implementation uses a sampling approach rather than throttling every individual byte, which provides better performance while maintaining accurate rate control over time. The throttling logic is implemented in the `throttle()` function, which is called by both archive and manifest content processing methods.

## Parameters / Member Variables
- `base`: The common bbsink structure containing standard sink operations, buffer information, and pointer to the next sink in the chain
- `throttling_sample`: Number of bytes that can be transferred before checking if throttling is needed (calculated as maxrate * 1024 / THROTTLING_FREQUENCY)
- `throttling_counter`: Running counter of bytes transferred since the last throttling check
- `elapsed_min_unit`: Minimum time that should elapse when transferring `throttling_sample` bytes (calculated as USECS_PER_SEC / THROTTLING_FREQUENCY)
- `throttled_last`: Timestamp of the last throttling measurement, used to calculate elapsed time

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (base struct)
  - TimeOffset (for time calculations)
  - TimestampTz (for timestamp storage)
- Called from (representative examples):
  - bbsink_throttle_new
  - bbsink_throttle_begin_backup
  - bbsink_throttle_archive_contents
  - bbsink_throttle_manifest_contents
  - throttle

## Notes and Other Information
- The throttling mechanism operates at a frequency of 8 samples per second (THROTTLING_FREQUENCY = 8)
- Rate limiting is applied to both archive contents and manifest contents during backup operations
- The struct forwards most operations unchanged to the next sink in the chain, only intercepting content transfer operations
- Sleep operations use PostgreSQL's latch mechanism with timeout, allowing for proper interrupt handling during backups
- The throttling counter is reset modulo the sample size after each throttling check to maintain accuracy across multiple samples