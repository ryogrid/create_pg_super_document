# phraseto_tsquery

## Location
[src/backend/tsearch/to_tsany.c:680-691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L680-L691)

## Overview
A user-facing function that converts plain text to a phrase TSQuery using the current default text search configuration.

## Definition
```c
Datum phraseto_tsquery(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a convenient wrapper around phraseto_tsquery_byid() that uses the current default text search configuration instead of requiring the user to specify a configuration OID. It automatically retrieves the current text search configuration using getTSCurrentConfig() and then delegates the actual phrase query parsing to phraseto_tsquery_byid().

This function is the primary entry point for users who want to convert plain text input into a phrase text search query without having to deal with text search configuration details. It's designed for cases where users want exact phrase matching using the default configuration.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: Input text to be converted into a phrase TSQuery

## Dependencies
- Functions called/Symbols referenced:
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md) (retrieves current text search configuration)
  - [phraseto_tsquery_byid](phraseto_tsquery_byid.md) (actual implementation function)
  - DirectFunctionCall2 (PostgreSQL function call mechanism)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)
  - [PointerGetDatum](../P/PointerGetDatum.md) (pointer to Datum conversion)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is the main user-facing SQL function for plain text to phrase TSQuery conversion
- Uses DirectFunctionCall2 to efficiently call the underlying phraseto_tsquery_byid function
- Automatically uses the current session's default text search configuration, making it user-friendly
- Creates queries that require exact phrase matching, which is more restrictive than plainto_tsquery
- Part of PostgreSQL's text search functionality that allows precise phrase searches without requiring knowledge of TSQuery syntax
- The function is typically accessed through SQL as phraseto_tsquery('exact phrase text')
- Useful when users need to find documents containing exact phrases rather than just all the words

## Simplified Source

```c
Datum
phraseto_tsquery(PG_FUNCTION_ARGS)
{
    text *input_text = PG_GETARG_TEXT_PP(0);

    // Get current default text search configuration
    Oid config_id = getTSCurrentConfig(true);

    // Delegate to phraseto_tsquery_byid with default config
    PG_RETURN_DATUM(DirectFunctionCall2(phraseto_tsquery_byid,
                                       ObjectIdGetDatum(config_id),
                                       PointerGetDatum(input_text)));
}
```