# SPI_cursor_open

## Location
[src/backend/executor/spi.c:1445-1471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1445-L1471)

## Overview
SPI_cursor_open opens a prepared SPI plan as a portal (cursor) that allows for incremental fetching of query results, providing memory-efficient access to large result sets.

## Definition
```c
Portal SPI_cursor_open(const char *name, SPIPlanPtr plan, Datum *Values, const char *Nulls, bool read_only)
```

## Detailed Description
This function creates a portal (cursor) from a previously prepared SPI plan, enabling incremental result fetching rather than loading all results into memory at once. The function acts as a wrapper around SPI_cursor_open_internal, handling parameter conversion from the traditional Datum/Nulls format to PostgreSQL's internal ParamListInfo format.

The function performs the following key operations:
1. **Parameter Conversion**: Converts the provided Datum array and Nulls string into a ParamListInfo structure using _SPI_convert_params
2. **Portal Creation**: Calls the internal cursor opening function with the converted parameters
3. **Memory Management**: Cleans up the transient ParamListInfo structure after portal creation

This approach provides a convenient interface for applications that work with the traditional SPI parameter format while leveraging the more efficient internal parameter representation.

## Parameters / Member Variables
- `name`: Name to assign to the portal/cursor. Can be NULL for an unnamed portal.
- `plan`: Pointer to a previously prepared SPI plan (SPIPlanPtr) containing the compiled query.
- `Values`: Array of Datum values for plan parameters. Can be NULL if the plan has no parameters.
- `Nulls`: String indicating which parameters are NULL ('n' for null, ' ' for non-null). Can be NULL if no parameters are null.
- `read_only`: Boolean flag indicating whether the cursor should be read-only.

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_convert_params](_SPI_convert_params.md) (convert parameters to internal format)
  - [SPI_cursor_open_internal](SPI_cursor_open_internal.md) (internal portal creation)
  - [pfree](../p/pfree.md) (free converted parameter list)
- Called from (representative examples):
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md) (text search query rewriting)
  - [ts_stat_sql](../t/ts_stat_sql.md) (text search statistics)
  - [query_to_xmlschema](../q/query_to_xmlschema.md) (XML schema generation)
  - [plperl_spi_query](../p/plperl_spi_query.md) (Perl procedural language)
  - [PLy_cursor_query](../P/PLy_cursor_query.md) (Python procedural language)

## Notes and Other Information
- The function creates a transient ParamListInfo structure that is immediately freed after use, making it safe for repeated calls.
- [Portal](../P/Portal.md) names must be unique within a transaction; using duplicate names will result in an error.
- Read-only cursors provide better performance and safety for queries that don't modify data.
- The returned Portal can be used with SPI_cursor_fetch to retrieve results incrementally.
- This function is commonly used in procedural languages and applications that need to process large result sets without loading everything into memory.

## Simplified Source

```c
Portal SPI_cursor_open(const char *name, SPIPlanPtr plan,
                      Datum *Values, const char *Nulls, bool read_only) {
    Portal portal;
    ParamListInfo paramLI;

    // Convert traditional parameters to internal format
    paramLI = _SPI_convert_params(plan->nargs, plan->argtypes, Values, Nulls);

    // Create the cursor portal
    portal = SPI_cursor_open_internal(name, plan, paramLI, read_only);

    // Clean up temporary parameter structure
    if (paramLI)
        pfree(paramLI);

    return portal;
}
```