# check_recovery_target_time

## Location
src/backend/access/transam/xlogrecovery.c: 4895 - 4949

## Overview
A GUC (Grand Unified Configuration) check hook function that validates the `recovery_target_time` parameter, performing syntax validation on timestamp strings while rejecting special relative time values.

## Definition
```c
bool check_recovery_target_time(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook for the `recovery_target_time` PostgreSQL configuration parameter. It performs comprehensive timestamp parsing to catch syntax errors in time specifications for point-in-time recovery. The function first rejects special relative time keywords like "now", "today", "tomorrow", and "yesterday" that could lead to ambiguous recovery targets. For valid timestamp strings, it uses PostgreSQL's internal datetime parsing functions (`ParseDateTime` and `DecodeDateTime`) to validate the format and convert it to a timestamp. The function performs preliminary parsing but doesn't store the parsed result since timezone settings might change the interpretation later - the actual parsing is deferred until the value is needed during recovery. This approach ensures syntax validation while allowing for proper timezone handling.

## Parameters / Member Variables
- `newval`: Pointer to the new string value being assigned to the GUC parameter (timestamp string)
- `extra`: Pointer for storing additional data (unused since parsing is deferred)
- `source`: The source of the GUC setting (e.g., configuration file, command line, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - ParseDateTime (parses datetime string into fields)
  - DecodeDateTime (interprets parsed fields into datetime components)
  - tm2timestamp (converts time structure to timestamp)
  - GUC_check_errdetail (provides detailed error messages)
  - fsec_t, pg_tm, DateTimeErrorExtra (datetime-related types)
  - MAXDATEFIELDS, MAXDATELEN, DTK_DATE (datetime parsing constants)
  - GucSource (enum type for configuration source)
- Called from (representative examples):
  - PostgreSQL GUC system when recovery_target_time parameter is being set

## Notes and Other Information
- This is part of PostgreSQL's point-in-time recovery (PITR) system for time-based recovery targets
- The function explicitly rejects relative time keywords to ensure deterministic recovery behavior
- Performs thorough syntax validation using PostgreSQL's internal datetime parsing infrastructure  
- Unlike other recovery target check hooks, this one doesn't store parsed data due to timezone dependency
- The actual timestamp parsing for recovery happens later when timezone context is fully established
- Supports standard PostgreSQL timestamp formats including ISO 8601 and PostgreSQL-specific formats
- Empty strings are accepted, allowing the recovery target time to be unset
- The validation ensures timestamps are within PostgreSQL's supported range