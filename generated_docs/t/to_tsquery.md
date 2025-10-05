# to_tsquery

## Location
[src/backend/tsearch/to_tsany.c:605-616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L605-L616)

## Overview
A PostgreSQL function that converts text input to a TSQuery using the current default text search configuration.

## Definition
```c
Datum to_tsquery(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a convenient wrapper for creating TSQuery objects from text input without explicitly specifying a text search configuration. It automatically retrieves the current default text search configuration and delegates the actual conversion work to `to_tsquery_byid`. The function follows PostgreSQL's pattern of providing both explicit configuration and default configuration variants of text search functions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (text *): Input text to be converted to TSQuery

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md)
  - DirectFunctionCall2
  - [to_tsquery_byid](to_tsquery_byid.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - PG_RETURN_DATUM
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Convenience function that uses the default text search configuration rather than requiring explicit configuration specification
- Implements the common PostgreSQL pattern of providing both `function_byid` and `function` variants
- Uses DirectFunctionCall2 to invoke to_tsquery_byid with the retrieved default configuration
- Part of PostgreSQL's SQL-accessible text search API for end users who don't need explicit configuration control
- The getTSCurrentConfig(true) call ensures that a configuration is available and will raise an error if none is set

## Simplified Source

```c
Datum to_tsquery(PG_FUNCTION_ARGS)
{
    // Extract input text
    text *in = PG_GETARG_TEXT_PP(0);

    // Get current default text search configuration
    Oid cfgId = getTSCurrentConfig(true);

    // Delegate to to_tsquery_byid with default configuration
    PG_RETURN_DATUM(DirectFunctionCall2(to_tsquery_byid,
                                        ObjectIdGetDatum(cfgId),
                                        PointerGetDatum(in)));
}
```