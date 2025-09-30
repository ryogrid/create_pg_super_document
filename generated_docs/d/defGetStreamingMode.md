# defGetStreamingMode

## Location
[src/backend/commands/subscriptioncmds.c:2391-2441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L2391-L2441)

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
  - [DefElem](../D/DefElem.md)
  - LOGICALREP_STREAM_ON
  - nodeTag
  - intVal
  - LOGICALREP_STREAM_OFF
  - [defGetString](defGetString.md)
  - LOGICALREP_STREAM_PARALLEL
- Called from (representative examples):
  - [parse_subscription_options](../p/parse_subscription_options.md)
  - [parse_output_parameters](../p/parse_output_parameters.md)
  - SUBSCRIPTIONCMDS_H

## Notes and Other Information
- Defaults to `LOGICALREP_STREAM_ON` when no parameter value is provided
- Accepts the same string values as the grammar's `opt_boolean_or_string` production
- The "parallel" option enables parallel apply workers for logical replication
- Uses case-insensitive string comparison via `pg_strcasecmp`
- Provides descriptive error messages that include the parameter name
- Part of PostgreSQL's logical replication infrastructure for subscription management
- The function ensures robust parameter validation for streaming mode configuration

## Simplified Source

```c
char
defGetStreamingMode(DefElem *def)
{
    // Default to streaming enabled if no parameter provided
    if (!def->arg)
        return LOGICALREP_STREAM_ON;

    // Handle different parameter types
    switch (nodeTag(def->arg))
    {
        case T_Integer:
            switch (intVal(def->arg))
            {
                case 0:
                    return LOGICALREP_STREAM_OFF;
                case 1:
                    return LOGICALREP_STREAM_ON;
                default:
                    break;  // Fall through to error
            }
            break;

        default:
            {
                char *sval = defGetString(def);

                // Check for boolean values
                if (pg_strcasecmp(sval, "false") == 0 ||
                    pg_strcasecmp(sval, "off") == 0)
                    return LOGICALREP_STREAM_OFF;
                if (pg_strcasecmp(sval, "true") == 0 ||
                    pg_strcasecmp(sval, "on") == 0)
                    return LOGICALREP_STREAM_ON;
                if (pg_strcasecmp(sval, "parallel") == 0)
                    return LOGICALREP_STREAM_PARALLEL;
            }
            break;
    }

    // Invalid value - report error
    ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("%s requires a Boolean value or \"parallel\"",
                    def->defname)));
    return LOGICALREP_STREAM_OFF;
}
```