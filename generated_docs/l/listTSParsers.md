# listTSParsers

## Location
[src/bin/psql/describe.c:5147-5198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5147-L5198)

## Overview
The  function implements the  psql command for displaying text search parser information in a PostgreSQL database.

## Definition

```c
bool
listTSParsers(const char *pattern, bool verbose)
```
## Detailed Description
This function queries the  system catalog to retrieve and display information about text search parsers defined in the database. Text search parsers are components of PostgreSQL's full-text search functionality that break documents into tokens for indexing and searching. Each parser defines how to recognize and classify different types of text elements.

The function provides a simple listing mode that shows schema, name, and description of parsers. When verbose mode is requested, it delegates to  which provides more detailed information about parser functions and token types.

The query joins with the namespace catalog to show schema information and uses the visibility function to respect search path settings.

## Parameters / Member Variables
- `*pattern`: A SQL name pattern (with optional wildcards) to filter which text search parsers to display. If NULL, all visible parsers are shown.
- `verbose`: If true, delegates to  for detailed parser information; if false, shows basic parser listing with schema, name, and description.
## Dependencies
- Functions called/Symbols referenced:
  - : Called when verbose output is requested for detailed parser information
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Validates and processes SQL name patterns with wildcards
  - : Executes the constructed SQL query
  - : Formats and displays the query results
  - : Cleans up the string buffer
  - : Frees the query result
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Uses error handling with cleanup on validation failures
- The basic mode shows three columns: Schema, Name, and Description
- Parser visibility is determined by  function
- Results are ordered by schema name and parser name
- Text search parsers are essential components for full-text search functionality
- Common built-in parsers include 'default' parser for general text processing
- Custom parsers can be created for specialized document types or languages
- The function serves as a gateway to either simple or verbose parser information depending on the verbose parameter

## Simplified Source

```c
bool listTSParsers(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Delegate to verbose function if verbose mode requested
    if (verbose) {
        return listTSParsersVerbose(pattern);
    }

    // Initialize query buffer
    initPQExpBuffer(&buf);

    // Build SELECT query for basic parser information
    printfPQExpBuffer(&buf,
        "SELECT n.nspname as \"Schema\", "
        "p.prsname as \"Name\", "
        "pg_catalog.obj_description(p.oid, 'pg_ts_parser') as \"Description\" "
        "FROM pg_catalog.pg_ts_parser p "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.prsnamespace");

    // Validate and add pattern matching
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                               "n.nspname", "p.prsname", NULL,
                               "pg_catalog.pg_ts_parser_is_visible(p.oid)",
                               NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    // Add ordering
    appendPQExpBufferStr(&buf, " ORDER BY 1, 2;");

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res) return false;

    // Configure and display results
    myopt.title = "List of text search parsers";
    myopt.translate_header = true;
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;
}
```