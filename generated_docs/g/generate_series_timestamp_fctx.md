# generate_series_timestamp_fctx

## Location
[src/backend/utils/adt/timestamp.c:63-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L63-L71)

## Overview
This struct serves as a context structure for the generate_series_timestamp function, maintaining state across multiple function calls in PostgreSQL's set-returning function (SRF) framework for generating timestamp series.

## Definition

```c
typedef struct
{
	TimestampTz current;
	TimestampTz finish;
	Interval	step;
	int			step_sign;
	pg_tz	   *attimezone;
} generate_series_timestamptz_fctx;
```
## Detailed Description
The generate_series_timestamp_fctx structure is used as a function context (fctx) to preserve state between successive calls to the generate_series_timestamp function. This is essential for PostgreSQL's set-returning function mechanism, which generates a series of timestamp values incrementally across multiple function invocations. The structure stores the current position in the series, the end condition, the step interval, and the direction of iteration.

## Parameters / Member Variables
- `current`: The current timestamp value in the series generation process
- `finish`: The final timestamp value that marks the end of the series
- `step`: The interval by which to increment/decrement between consecutive timestamp values in the series
- `step_sign`: An integer indicating the direction of iteration (positive for forward, negative for backward)

## Dependencies
- Functions called/Symbols referenced:
  - Timestamp
  - Interval
- Called from (representative examples):
  - [generate_series_timestamp](generate_series_timestamp.md) (at src/backend/utils/adt/timestamp.c:6509)

## Notes and Other Information
- This struct is allocated using palloc() in the multi-call memory context to persist across function calls
- It's specifically used for timestamp (without timezone) series generation, as opposed to generate_series_timestamptz_fctx which includes timezone information
- The step_sign member is used to optimize the iteration logic by pre-calculating the direction of the series
- Memory management is handled by PostgreSQL's SRF framework, which automatically cleans up the context when the series generation is complete