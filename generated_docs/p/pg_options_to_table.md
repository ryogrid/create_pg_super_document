# pg_options_to_table

## Location
[src/backend/foreign/foreign.c:522-563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L522-L563)

## Overview
Converts options array to a name/value table format, useful for providing details to information_schema and pg_dump.

## Definition

```c
struct ConnectionOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};
```
## Detailed Description
The  function is a PostgreSQL SQL-callable function that takes an array of options and converts them into a tabular format with name-value pairs. This function is primarily designed to support introspection queries and database dumping operations by transforming internal option representations into a user-friendly table format.

The function processes each option in the input array, extracting the option name and value (if present), and returns them as a set of rows with two columns: option name and option value. Options without explicit values are represented with NULL in the value column.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention that provides access to:
  - : Input Datum representing the options array to be converted
  - : Return set information structure for handling multiple result rows

## Dependencies
- Functions called/Symbols referenced:
  - : Converts options array into internal List format
  - : Initializes materialized set-returning function context
  - : Converts C strings to PostgreSQL text datums
  - : Extracts string value from option argument
  - : Stores result row in tuple store
- Called from:
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is designed to be called from SQL as a set-returning function
- Uses PostgreSQL's materialized SRF (Set-Returning Function) framework
- The function expects a two-column result set with text columns for option names and values
- NULL values are used for options that don't have explicit arguments
- The function is primarily used by system catalogs and administrative utilities for option introspection
- Located in src/backend/foreign/foreign.c:522-563

## Simplified Source

```c
Datum
pg_options_to_table(PG_FUNCTION_ARGS)
{
    Datum array = PG_GETARG_DATUM(0);
    List *options;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    ListCell *cell;

    // Convert options array to internal list format
    options = untransformRelOptions(array);

    // Initialize set-returning function
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

    // Process each option and add to result set
    foreach(cell, options)
    {
        DefElem *def = lfirst(cell);
        Datum values[2];
        bool nulls[2];

        // Set option name
        values[0] = CStringGetTextDatum(def->defname);
        nulls[0] = false;

        // Set option value (or NULL if no value)
        if (def->arg) {
            values[1] = CStringGetTextDatum(strVal(def->arg));
            nulls[1] = false;
        } else {
            values[1] = (Datum) 0;
            nulls[1] = true;
        }

        // Add row to result set
        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
                             values, nulls);
    }

    return (Datum) 0;
}
```