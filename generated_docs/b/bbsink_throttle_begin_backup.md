# bbsink_throttle_begin_backup

## Location
[src/backend/backup/basebackup_throttle.c:96-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_throttle.c#L96-L109)

## Overview
Initializes the throttling sink for backup operations by recording the current timestamp as the starting point for throttling calculations.

## Definition

```c
static void
bbsink_throttle_begin_backup(bbsink *sink)
```
## Detailed Description
The  function serves as the initialization callback for the throttling basebackup sink when backup operations begin. While it performs minimal actual work, it plays a crucial role in establishing the timing baseline for the throttling mechanism. The function forwards the begin_backup call to the next sink in the pipeline and then records the current timestamp, which will be used as the reference point for subsequent throttling rate calculations.

This function marks the transition point where 'real data' transfer begins, as opposed to header information which is typically not subject to throttling controls.

## Parameters / Member Variables
- `*sink`: Pointer to the base bbsink structure, which is cast to bbsink_throttle for accessing throttling-specific members
## Dependencies
- Functions called/Symbols referenced:
  -  (forwards the begin backup call to next sink)
  -  (records current time for throttling calculations)
  -  (structure type for casting)
- Called from (representative examples):
  - Used as callback through bbsink_throttle_ops function pointer table

## Notes and Other Information
- This is a static function, indicating it's only used within the basebackup_throttle.c module
- The function is part of the bbsink operation callback interface
- The timestamp recorded here () becomes the baseline for measuring transfer rates
- Header data transmitted before this point is not included in throttling calculations
- The function ensures proper initialization of the throttling mechanism's timing system

## Simplified Source

```c
static void bbsink_throttle_begin_backup(bbsink *sink) {
    bbsink_throttle *mysink = (bbsink_throttle *) sink;

    // Forward the begin backup call to next sink in chain
    bbsink_forward_begin_backup(sink);

    // Record current time as baseline for throttling calculations
    mysink->throttled_last = GetCurrentTimestamp();
}
```