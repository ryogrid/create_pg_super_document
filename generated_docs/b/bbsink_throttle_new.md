# bbsink_throttle_new

## Location
[src/backend/backup/basebackup_throttle.c:68-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_throttle.c#L68-L95)

## Overview
Creates a new basebackup sink that performs bandwidth throttling and forwards data to a successor sink in the basebackup pipeline.

## Definition

```c
bbsink *
bbsink_throttle_new(bbsink *next, uint32 maxrate)
```
## Detailed Description
The  function creates and initializes a throttling basebackup sink that controls the rate of data transfer during base backups. It allocates and configures a  structure with throttling parameters based on the specified maximum transfer rate. The sink operates by calculating a throttling sample size and minimum time unit to regulate data flow, ensuring backup operations don't overwhelm system resources or network bandwidth.

The function sets up the throttling mechanism by:
- Calculating the throttling sample size based on the maximum rate and throttling frequency
- Computing the minimum time unit for transferring the sample data
- Establishing the sink operation callbacks through 

## Parameters / Member Variables
- `*next`: Pointer to the next sink in the basebackup pipeline chain that will receive forwarded data
- `maxrate`: Maximum transfer rate in kilobytes per second for the throttling mechanism
## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (structure type)
  -  (operation callbacks)
  -  (constant for throttling calculations)
  -  (time conversion constant)
- Called from (representative examples):
  -  (src/backend/backup/basebackup.c:1038)
  -  (src/include/backup/basebackup_sink.h:292)

## Notes and Other Information
- The function uses assertions to ensure valid parameters (non-null next sink and positive maxrate)
- Throttling calculations convert the rate from KB/s to bytes per throttling interval
- The throttling mechanism operates at a frequency defined by 
- Memory is allocated using  to ensure zero-initialization of the structure
- Part of PostgreSQL's basebackup throttling subsystem for controlling backup transfer rates

## Simplified Source

```c
// Simplified version of bbsink_throttle_new
bbsink *bbsink_throttle_new(bbsink *next, uint32 maxrate) {
    bbsink_throttle *sink;

    // Validate parameters
    Assert(next != NULL);
    Assert(maxrate > 0);

    // Allocate and initialize throttle sink structure
    sink = palloc0(sizeof(bbsink_throttle));

    // Set up operations table for throttling functionality
    *((const bbsink_ops **) &sink->base.bbs_ops) = &bbsink_throttle_ops;

    // Chain to next sink in pipeline
    sink->base.bbs_next = next;

    // Calculate throttling sample size
    // Convert maxrate (KB/s) to bytes per throttling interval
    sink->throttling_sample =
        (int64) maxrate * (int64) 1024 / THROTTLING_FREQUENCY;

    // Calculate minimum time unit for throttling sample transfer
    // Time in microseconds for one throttling interval
    sink->elapsed_min_unit = USECS_PER_SEC / THROTTLING_FREQUENCY;

    return &sink->base;
}
```

Key simplifications made:
- Added clear comments explaining throttling calculations
- Preserved all validation logic and parameter checking
- Maintained the throttling algorithm setup
- Kept sink chaining and operations table setup intact
- Simplified structure while preserving all functionality
- Clarified the conversion from KB/s to bytes per interval