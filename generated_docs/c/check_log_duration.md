# check_log_duration

## Location
src/backend/tcop/postgres.c: 2407 - 2469

## Overview
Determines whether the current command's execution duration should be logged based on various duration-related logging configuration parameters.

## Definition


## Detailed Description
This function implements PostgreSQL's duration-based logging policy by measuring statement execution time and determining the appropriate logging action based on multiple configuration parameters:

- : Log all statement durations regardless of time
- : Log statements exceeding specified duration threshold
- : Log sampled statements exceeding specified duration threshold
- : Probabilistic sampling rate for duration-based logging
- : Transaction-level sampling flag

The function calculates execution duration from statement start to current time and applies complex logic to determine logging requirements. It uses careful arithmetic to avoid integer overflow with very long durations and implements probabilistic sampling using PostgreSQL's global PRNG.

## Parameters / Member Variables
- : 32-byte buffer to store formatted duration string (output parameter)
- : Boolean indicating if statement details were already logged by caller

## Dependencies
- Functions called/Symbols referenced:
  - TimestampDifference (calculate time difference between timestamps)
  - GetCurrentStatementStartTimestamp (get statement start time)
  - GetCurrentTimestamp (get current timestamp)
  - pg_prng_double (generate random number for sampling)
- Called from (representative examples):
  - exec_simple_query (simple query execution)
  - exec_parse_message (Parse message processing)
  - exec_bind_message (Bind message processing)
  - exec_execute_message (Execute message processing)
  - HandleFunctionRequest (function call processing)

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