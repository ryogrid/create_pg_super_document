# fill_ident_line

## Location
[src/backend/utils/adt/hbafuncs.c:468-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/hbafuncs.c#L468-L520)

## Overview
Builds one row of the pg_ident_file_mappings system view and adds it to a tuplestore, representing a single identity mapping rule from the pg_ident.conf file.

## Definition

```c
static void
fill_ident_line(Tuplestorestate *tuple_store, TupleDesc tupdesc,
				int map_number, char *filename, int lineno, IdentLine *ident,
				const char *err_msg)
```
## Detailed Description
The  function constructs a complete row for the pg_ident_file_mappings system view, which exposes PostgreSQL's user name mapping configuration to SQL queries. It processes a parsed identity mapping line structure and extracts the mapping name, system user pattern, and PostgreSQL user name. The function handles both successful parsing results and error conditions, formatting the data into SQL-compatible types. Like its HBA counterpart, it accepts memory leaks since execution occurs in a short-lived memory context. The function provides visibility into how external authentication systems map their user identities to PostgreSQL roles.

## Parameters / Member Variables
- `*tuple_store`: Tuplestore where the constructed mapping row will be added
- `tupdesc`: Tuple descriptor defining the structure of the pg_ident_file_mappings view
- `map_number`: Unique identifier for valid identity mapping rules (NULL for invalid rules)
- `*filename`: Name of the identity mapping configuration file (always valid)
- `lineno`: Line number within the configuration file (always valid)
- `*ident`: Parsed identity mapping line data structure (can be NULL for parsing errors)
- `*err_msg`: Error message for invalid mapping rules (NULL if rule is valid)
## Dependencies
- Functions called/Symbols referenced:
  - [Int32GetDatum](../I/Int32GetDatum.md), CStringGetTextDatum
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [tuplestore_puttuple](../t/tuplestore_puttuple.md)
  - Assert, memset
- Types referenced:
  - [Tuplestorestate](../T/Tuplestorestate.md), TupleDesc, IdentLine
  - NUM_PG_IDENT_FILE_MAPPINGS_ATTS
  - HeapTuple, Datum
- Called from:
  - [fill_ident_view](fill_ident_view.md)

## Notes and Other Information
- Function is static and only used within hbafuncs.c for system view implementation
- Simpler than fill_hba_line due to the more straightforward structure of identity mappings
- Handles both valid and invalid identity mapping lines appropriately
- Extracts usermap name, system user pattern, and PostgreSQL user from IdentLine structure
- Uses PostgreSQL's standard tuple construction and storage mechanisms
- Memory management is simplified due to short-lived execution context
- Part of PostgreSQL's system view infrastructure for configuration introspection
- Supports the same error reporting pattern as other configuration view functions

## Simplified Source

```c
static void
fill_ident_line(Tuplestorestate *tuple_store, TupleDesc tupdesc,
                int map_number, char *filename, int lineno, IdentLine *ident,
                const char *err_msg)
{
    Datum values[NUM_PG_IDENT_FILE_MAPPINGS_ATTS];
    bool nulls[NUM_PG_IDENT_FILE_MAPPINGS_ATTS];
    int index = 0;

    memset(values, 0, sizeof(values));
    memset(nulls, 0, sizeof(nulls));

    // Map number (NULL on error)
    if (err_msg)
        nulls[index++] = true;
    else
        values[index++] = Int32GetDatum(map_number);

    // File name and line number
    values[index++] = CStringGetTextDatum(filename);
    values[index++] = Int32GetDatum(lineno);

    if (ident != NULL) {
        // Identity mapping data: usermap, system_user, pg_user
        values[index++] = CStringGetTextDatum(ident->usermap);
        values[index++] = CStringGetTextDatum(ident->system_user->string);
        values[index++] = CStringGetTextDatum(ident->pg_user->string);
    } else {
        // Set mapping fields to NULL for parse errors
        memset(&nulls[3], true, (NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 4) * sizeof(bool));
    }

    // Error message
    if (err_msg)
        values[NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1] = CStringGetTextDatum(err_msg);
    else
        nulls[NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1] = true;

    // Create and store tuple
    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    tuplestore_puttuple(tuple_store, tuple);
}
```