# generate_series_timestamptz_internal

## Location
[src/backend/utils/adt/timestamp.c:6590-6671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6590-L6671)

## Overview
An internal function that implements the core logic for generating a series of timestamp values with time zone, performing arithmetic operations in a specified timezone.

## Definition

```c
static Datum
generate_series_timestamptz_internal(FunctionCallInfo fcinfo)
```
## Detailed Description
This function implements a set-returning function (SRF) that generates a sequence of timestamp with timezone values from a start point to a finish point, incrementing by a specified interval step. The function operates in a specified timezone (either provided as parameter or using session timezone) and handles both positive and negative step intervals.

The function uses PostgreSQL's SRF framework to maintain state between calls, storing the current position, finish point, step interval, timezone information, and step direction. On each invocation, it returns the next timestamp in the series until the finish condition is met.

Key behaviors:
- Validates that step size is non-zero and finite
- Handles both forward (positive step) and backward (negative step) series generation  
- Performs timezone-aware interval arithmetic using timestamptz_pl_interval_internal
- Uses efficient timestamp comparison to determine series completion

## Parameters / Member Variables
- : Function call information containing:
  - : Start timestamp with timezone (TimestampTz)
  - : Finish timestamp with timezone (TimestampTz) 
  - : Step interval (Interval *)
  - : Optional timezone specification (text *), uses session_timezone if not provided

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP (SRF framework macros)
  - PG_GETARG_TIMESTAMPTZ, PG_GETARG_INTERVAL_P, PG_NARGS (argument extraction)
  - [lookup_timezone](../l/lookup_timezone.md) (timezone resolution)
  - [interval_sign](../i/interval_sign.md) (interval direction determination)
  - INTERVAL_NOT_FINITE (interval validation macro)
  - [timestamp_cmp_internal](../t/timestamp_cmp_internal.md) (timestamp comparison)
  - [timestamptz_pl_interval_internal](../t/timestamptz_pl_interval_internal.md) (timezone-aware interval addition)
  - TimestampTzGetDatum (datum conversion)
- Called from:
  - [generate_series_timestamptz](generate_series_timestamptz.md) (src/backend/utils/adt/timestamp.c:6674)
  - [generate_series_timestamptz_at_zone](generate_series_timestamptz_at_zone.md) (src/backend/utils/adt/timestamp.c:6680)

## Notes and Other Information
- This is a static internal function, not directly callable from SQL
- Uses PostgreSQL's SRF (Set Returning Function) framework for stateful iteration
- Performs all timestamp arithmetic in the specified timezone context rather than UTC
- Validates step parameters to prevent infinite loops (zero step) or invalid operations (infinite step)
- Memory management handled through PostgreSQL's memory context system
- Function state preserved in generate_series_timestamptz_fctx structure across calls