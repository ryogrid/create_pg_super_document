# defGetStreamingMode

## Location
src/backend/commands/subscriptioncmds.c: 2391 - 2441

## Overview
Extracts and validates streaming mode values from DefElem parameters, supporting boolean values and the special "parallel" mode for logical replication subscriptions.

## Definition
```c
char defGetStreamingMode(DefElem *def)
```

## Detailed Description
The `defGetStreamingMode` function is a specialized parameter parsing utility that extends the functionality of `defGetBoolean()` to handle logical replication streaming modes. It parses DefElem parameters to extract streaming configuration values, supporting traditional boolean values (true/false, on/off, 1/0) as well as the special "parallel" streaming mode introduced for parallel logical replication.

The function handles various input formats including integers, strings, and missing parameters (defaulting to streaming enabled). It validates the input and returns one of the predefined logical replication streaming mode constants.

## Parameters
- `def`: DefElem pointer containing the parameter definition to parse

## Return Values
- `LOGICALREP_STREAM_OFF`: Streaming disabled (false, off, 0)
- `LOGICALREP_STREAM_ON`: Normal streaming enabled (true, on, 1, or default when no arg)
- `LOGICALREP_STREAM_PARALLEL`: Parallel streaming mode ("parallel")

## Dependencies
- Functions called/Symbols referenced:
  - DefElem
  - LOGICALREP_STREAM_ON
  - nodeTag
  - intVal
  - LOGICALREP_STREAM_OFF
  - defGetString
  - LOGICALREP_STREAM_PARALLEL
- Called from (representative examples):
  - parse_subscription_options
  - parse_output_parameters
  - SUBSCRIPTIONCMDS_H

## Notes and Other Information
- Defaults to `LOGICALREP_STREAM_ON` when no parameter value is provided
- Accepts the same string values as the grammar's `opt_boolean_or_string` production
- The "parallel" option enables parallel apply workers for logical replication
- Uses case-insensitive string comparison via `pg_strcasecmp`
- Provides descriptive error messages that include the parameter name
- Part of PostgreSQL's logical replication infrastructure for subscription management
- The function ensures robust parameter validation for streaming mode configuration