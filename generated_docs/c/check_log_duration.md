# check_log_duration

## Location
[src/backend/tcop/postgres.c:2407-2469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2407-L2469)

## Overview
Determines whether the current command's execution duration should be logged based on various duration-related logging configuration parameters.

## Definition

```c
int
check_log_duration(char *msec_str, bool was_logged)
```
## Detailed Description
This function implements PostgreSQL's duration-based logging policy by measuring statement execution time and determining the appropriate logging action based on multiple configuration parameters:

- : Log all statement durations regardless of time
- : Log statements exceeding specified duration threshold
- : Log sampled statements exceeding specified duration threshold
- : Probabilistic sampling rate for duration-based logging
- : Transaction-level sampling flag

The function calculates execution duration from statement start to current time and applies complex logic to determine logging requirements. It uses careful arithmetic to avoid integer overflow with very long durations and implements probabilistic sampling using PostgreSQL's global PRNG.

## Parameters / Member Variables
- `*msec_str`: 32-byte buffer to store formatted duration string (output parameter)
- `was_logged`: Boolean indicating if statement details were already logged by caller
## Dependencies
- Functions called/Symbols referenced:
  - [TimestampDifference](../T/TimestampDifference.md) (calculate time difference between timestamps)
  - [GetCurrentStatementStartTimestamp](../G/GetCurrentStatementStartTimestamp.md) (get statement start time)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (get current timestamp)
  - [pg_prng_double](../p/pg_prng_double.md) (generate random number for sampling)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md) (simple query execution)
  - [exec_parse_message](../e/exec_parse_message.md) (Parse message processing)
  - [exec_bind_message](../e/exec_bind_message.md) (Bind message processing)
  - [exec_execute_message](../e/exec_execute_message.md) (Execute message processing)
  - [HandleFunctionRequest](../H/HandleFunctionRequest.md) (function call processing)

## Notes and Other Information
- **Return Values:**
  - 0: No logging needed
  - 1: Log duration only
  - 2: Log both duration and query details
- Implements overflow-safe duration calculation by avoiding premature multiplication
- Supports probabilistic sampling through log_statement_sample_rate configuration
- Integrates with transaction-level sampling via xact_is_sampled flag
- Formats duration as milliseconds with microsecond precision (e.g., "1234.567")
- Prevents duplicate query detail logging when was_logged parameter is true
- Critical component of PostgreSQL's performance monitoring and auditing infrastructure

## Simplified Source

```c
// Simplified version of check_log_duration
int check_log_duration(char *msec_str, bool was_logged) {
    // Skip if no duration logging is configured
    if (!log_duration && log_min_duration_sample < 0 &&
        log_min_duration_statement < 0 && !xact_is_sampled) {
        return 0;
    }

    // Calculate statement execution duration
    long secs;
    int usecs;
    TimestampDifference(GetCurrentStatementStartTimestamp(),
                       GetCurrentTimestamp(),
                       &secs, &usecs);
    int msecs = usecs / 1000;
    int total_duration_ms = secs * 1000 + msecs;

    // Check if duration exceeds configured thresholds
    bool exceeds_min_duration = (log_min_duration_statement >= 0 &&
                                total_duration_ms >= log_min_duration_statement);

    bool exceeds_sample_duration = (log_min_duration_sample >= 0 &&
                                   total_duration_ms >= log_min_duration_sample);

    // Apply probabilistic sampling for sample duration
    bool should_sample = false;
    if (exceeds_sample_duration && log_statement_sample_rate > 0) {
        should_sample = (log_statement_sample_rate == 1 ||
                        pg_prng_double(&pg_global_prng_state) <= log_statement_sample_rate);
    }

    // Determine if logging is needed
    bool needs_logging = (log_duration || exceeds_min_duration ||
                         should_sample || xact_is_sampled);

    if (needs_logging) {
        // Format duration string: "milliseconds.microseconds"
        snprintf(msec_str, 32, "%ld.%03d",
                secs * 1000 + msecs, usecs % 1000);

        // Return 2 for full logging (duration + query), 1 for duration only
        if ((exceeds_min_duration || should_sample || xact_is_sampled) && !was_logged) {
            return 2;  // Log duration and query details
        } else {
            return 1;  // Log duration only
        }
    }

    return 0;  // No logging needed
}
```

Key simplifications made:
- Removed complex overflow-prevention arithmetic for clarity
- Consolidated multiple duration threshold checks
- Simplified the sampling logic flow
- Added descriptive variable names and inline comments
- Focused on the main execution path
- Preserved the essential three-way return value logic