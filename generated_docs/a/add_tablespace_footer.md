# add_tablespace_footer

## Location
[src/bin/psql/describe.c:3549-3613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L3549-L3613)

## Overview
A utility function that adds tablespace information to the footer of table descriptions in psql's \d command output.

## Definition

```c
static void
add_tablespace_footer(printTableContent *const cont, char relkind,
					  Oid tablespace, const bool newline)
```
## Detailed Description
The  function is a specialized utility that enhances psql's describe output by adding tablespace information to relation descriptions. It only operates on relation types that support tablespaces and only displays non-default tablespaces to avoid cluttering the output for users not utilizing custom tablespaces.

The function performs a targeted query to pg_tablespace to retrieve the tablespace name and formats it appropriately for display. It provides two formatting modes: it can either add the tablespace information as a new footer line or append it to the existing footer content (useful for index descriptions where tablespace information is appended to the index definition).

The function includes proper error handling and resource cleanup, ensuring that any database query failures don't affect the overall describe operation.

## Parameters / Member Variables
- `cont`: Pointer to the printTableContent structure that manages the table formatting and footer information
- `relkind`: Character representing the relation kind (table, index, materialized view, etc.) to determine if tablespace information is applicable
- `tablespace`: OID of the tablespace to describe. If 0 (default tablespace), no information is displayed
- `newline`: Boolean flag controlling formatting - if true, adds tablespace info as a new footer line; if false, appends to the current footer
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md): Initialize buffer for SQL query construction
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md): Format SQL query to retrieve tablespace name
  - [PSQLexec](../P/PSQLexec.md): Execute the tablespace lookup query
  - [printTableAddFooter](../p/printTableAddFooter.md): Add new footer line to table display
  - [printTableSetFooter](../p/printTableSetFooter.md): Replace existing footer content
  - [termPQExpBuffer](../t/termPQExpBuffer.md): Clean up query buffer resources
  - [PQclear](../P/PQclear.md): Free PostgreSQL result set
- Called from (representative examples):
  - [describeOneTableDetails](../d/describeOneTableDetails.md): Multiple locations for different relation types (tables, indexes, toast tables)

## Notes and Other Information
- The function is marked static, indicating it's only used within the describe.c file
- Only displays tablespace information for relation types that support tablespaces: tables, materialized views, indexes, partitioned tables/indexes, and TOAST tables
- Intentionally ignores the default tablespace (OID 0) to avoid unnecessary information for users not using custom tablespaces
- Supports internationalization with proper gettext integration for translatable strings
- Includes translator comments to provide context for proper localization
- The dual formatting modes (newline vs append) allow for flexible integration with different types of relation descriptions
- Gracefully handles query failures by simply not adding tablespace information rather than failing the entire describe operation

## Simplified Source

```c
static void add_tablespace_footer(printTableContent *const cont, char relkind,
                                 Oid tablespace, const bool newline) {
    // Only show tablespace info for relation types that support tablespaces
    if (relkind == RELKIND_RELATION ||
        relkind == RELKIND_MATVIEW ||
        relkind == RELKIND_INDEX ||
        relkind == RELKIND_PARTITIONED_TABLE ||
        relkind == RELKIND_PARTITIONED_INDEX ||
        relkind == RELKIND_TOASTVALUE) {

        // Ignore default tablespace to avoid cluttering output
        if (tablespace != 0) {
            PGresult *result = NULL;
            PQExpBufferData buf;

            initPQExpBuffer(&buf);

            // Query to get tablespace name
            printfPQExpBuffer(&buf,
                "SELECT spcname FROM pg_catalog.pg_tablespace\n"
                "WHERE oid = '%u';", tablespace);

            result = PSQLexec(buf.data);
            if (!result) {
                termPQExpBuffer(&buf);
                return;
            }

            // Add tablespace info if found
            if (PQntuples(result) > 0) {
                if (newline) {
                    // Add as new footer line
                    printfPQExpBuffer(&buf, "Tablespace: \"%s\"",
                                     PQgetvalue(result, 0, 0));
                    printTableAddFooter(cont, buf.data);
                } else {
                    // Append to existing footer
                    printfPQExpBuffer(&buf, "%s", cont->footer->data);
                    appendPQExpBuffer(&buf, ", tablespace \"%s\"",
                                     PQgetvalue(result, 0, 0));
                    printTableSetFooter(cont, buf.data);
                }
            }

            PQclear(result);
            termPQExpBuffer(&buf);
        }
    }
}
```