# timestamptz_to_str

## Location
src/backend/utils/adt/timestamp.c: 1853 - 1873

## Overview
timestamptz_to_str is a utility function that converts a TimestampTz value into a C-string representation, primarily for use in emitting messages and logging.

## Definition
```c
const char *timestamptz_to_str(TimestampTz t)
```

## Detailed Description
This function produces a C-string representation of a TimestampTz value with the output format forced to ISO standard. It differs from timestamptz_out in that it always uses ISO format regardless of system settings and returns a pointer to a static buffer rather than a pstrdup'd string. The function handles special cases including infinite timestamps and out-of-range values. It is primarily designed for use in error messages, logging, and debugging output where a consistent, readable timestamp format is needed.

## Parameters / Member Variables
- `t`: The TimestampTz value to be converted to a string representation

## Dependencies
- Functions called/Symbols referenced:
  - MAXDATELEN (maximum date length constant)
  - [pg_tm](../p/pg_tm.md) (PostgreSQL time structure)
  - fsec_t (fractional seconds type)
  - TIMESTAMP_NOT_FINITE (macro to check for infinite timestamps)
  - [EncodeSpecialTimestamp](../E/EncodeSpecialTimestamp.md) (function to encode special timestamp values)
  - [timestamp2tm](timestamp2tm.md) (function to convert timestamp to broken-down time)
  - [EncodeDateTime](../E/EncodeDateTime.md) (function to encode datetime to string)
  - USE_ISO_DATES (constant for ISO date format)
  - strlcpy (safe string copy function)
- Called from (representative examples):
  - [xact_desc_commit](../x/xact_desc_commit.md) (transaction commit descriptions)
  - [xact_desc_abort](../x/xact_desc_abort.md) (transaction abort descriptions)
  - [xact_desc_prepare](../x/xact_desc_prepare.md) (transaction prepare descriptions)
  - [xlog_desc](../x/xlog_desc.md) (WAL log descriptions)
  - [CreateRestartPoint](../C/CreateRestartPoint.md) (restart point creation)
  - [InitWalRecovery](../I/InitWalRecovery.md) (WAL recovery initialization)
  - [ProcessWalSndrMessage](../P/ProcessWalSndrMessage.md) (WAL sender message processing)

## Notes and Other Information
- Returns a pointer to a static buffer, so the result is overwritten on subsequent calls
- Always uses ISO date format regardless of system DateStyle settings
- Handles infinite timestamps and out-of-range values gracefully
- Primarily used for debugging, logging, and error message generation
- The static buffer approach means the function is not thread-safe
- Commonly used in WAL-related logging and transaction processing messages
- Alternative to timestamptz_out when ISO format is specifically required
- Buffer size is limited to MAXDATELEN + 1 characters