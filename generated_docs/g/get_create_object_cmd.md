# get_create_object_cmd

## Location
[src/bin/psql/command.c:5666-5825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5666-L5825)

## Overview
Constructs complete "CREATE OR REPLACE" SQL commands for PostgreSQL database objects by retrieving their definitions from the system catalogs.

## Definition

```c
static bool
get_create_object_cmd(EditableObjectType obj_type, Oid oid,
					  PQExpBuffer buf)
```
## Detailed Description
The  function generates complete DDL (Data Definition Language) statements for PostgreSQL database objects that can be used to recreate them. It supports functions and views, handling the complexity of reconstructing proper CREATE OR REPLACE statements from system catalog information.

For functions, it uses pg_get_functiondef() which returns a complete CREATE OR REPLACE FUNCTION statement. For views, it performs more complex processing: it retrieves the view definition using pg_get_viewdef(), constructs the proper CREATE OR REPLACE VIEW prefix with schema-qualified names, handles reloptions (storage parameters), and processes CHECK OPTION settings. The function also includes version-specific handling for PostgreSQL 9.4+ features like LOCAL/CASCADED CHECK OPTION.

## Parameters / Member Variables
- `obj_type`: EditableObjectType enum specifying the type of object (EditableFunction, EditableView)
- `oid`: Object Identifier of the database object to retrieve
- `buf`: PQExpBuffer to store the resulting CREATE OR REPLACE statement
## Dependencies
- Functions called/Symbols referenced:
  - EditableObjectType (enum defining supported object types)
  - EditableFunction, EditableView (enum values for different object types)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formats SQL queries for system catalog lookups)
  - [echo_hidden_command](../e/echo_hidden_command.md) (displays query if ECHO_HIDDEN is enabled)
  - [PQexec](../P/PQexec.md) (executes the catalog query)
  - PGRES_TUPLES_OK (PostgreSQL result status constant)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md) (clears the output buffer)
  - RELKIND_VIEW, RELKIND_MATVIEW (constants for relation types)
  - [fmtId](../f/fmtId.md) (formats identifiers with proper quoting)
  - [appendReloptionsArray](../a/appendReloptionsArray.md) (processes view storage parameters)
  - [standard_strings](../s/standard_strings.md) (determines string literal handling)
  - [minimal_error_message](../m/minimal_error_message.md) (displays error information)
- Called from (representative examples):
  - [exec_command_ef_ev](../e/exec_command_ef_ev.md) (\ef and \ev commands for editing objects)
  - [exec_command_sf_sv](../e/exec_command_sf_sv.md) (\sf and \sv commands for showing object definitions)

## Notes and Other Information
- Handles PostgreSQL version differences (9.4+ CHECK OPTION support)
- For views, validates that the object is actually a view (not a table or other relation type)
- Processes view-specific features: reloptions, CHECK OPTION (LOCAL/CASCADED)
- Removes trailing semicolons from pg_get_viewdef() output for consistency
- Ensures output ends with a newline for proper formatting
- Does not currently support materialized views for CREATE OR REPLACE (marked with #ifdef NOT_USED)
- Fully qualifies view names to prevent ambiguity during recreation
- Essential for psql's object editing infrastructure, enabling users to modify and recreate database objects

## Simplified Source

```c
static bool
get_create_object_cmd(EditableObjectType obj_type, Oid oid, PQExpBuffer buf)
{
    bool result = true;
    PQExpBuffer query = createPQExpBuffer();
    PGresult *res;

    // Build appropriate query based on object type
    switch (obj_type) {
        case EditableFunction:
            // For functions, use built-in pg_get_functiondef
            printfPQExpBuffer(query, "SELECT pg_catalog.pg_get_functiondef(%u)", oid);
            break;

        case EditableView:
            // For views, need to construct CREATE statement manually
            // Handle version differences for CHECK OPTION support
            if (pset.sversion >= 90400) {
                printfPQExpBuffer(query,
                    "SELECT nspname, relname, relkind, "
                    "pg_catalog.pg_get_viewdef(c.oid, true), "
                    "reloptions, checkoption "
                    "FROM pg_catalog.pg_class c "
                    "LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
                    "WHERE c.oid = %u", oid);
            } else {
                // Older version without CHECK OPTION
                printfPQExpBuffer(query, "SELECT nspname, relname, relkind, "
                    "pg_catalog.pg_get_viewdef(c.oid, true), reloptions, NULL "
                    "FROM pg_catalog.pg_class c "
                    "LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
                    "WHERE c.oid = %u", oid);
            }
            break;
    }

    // Execute query and check for valid result
    if (!echo_hidden_command(query->data)) {
        destroyPQExpBuffer(query);
        return false;
    }

    res = PQexec(pset.db, query->data);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1) {
        resetPQExpBuffer(buf);

        switch (obj_type) {
            case EditableFunction:
                // Function definition is complete, just copy it
                appendPQExpBufferStr(buf, PQgetvalue(res, 0, 0));
                break;

            case EditableView:
                // Construct CREATE OR REPLACE VIEW statement
                char *schema = PQgetvalue(res, 0, 0);
                char *name = PQgetvalue(res, 0, 1);
                char *kind = PQgetvalue(res, 0, 2);
                char *definition = PQgetvalue(res, 0, 3);
                char *options = PQgetvalue(res, 0, 4);
                char *check_opt = PQgetvalue(res, 0, 5);

                // Verify it's actually a view
                if (kind[0] != RELKIND_VIEW) {
                    pg_log_error("\"%s.%s\" is not a view", schema, name);
                    result = false;
                    break;
                }

                // Build CREATE OR REPLACE VIEW statement
                appendPQExpBuffer(buf, "CREATE OR REPLACE VIEW %s.%s",
                    fmtId(schema), fmtId(name));

                // Add reloptions if present
                if (options && strlen(options) > 2) {
                    appendPQExpBufferStr(buf, "\n WITH (");
                    appendReloptionsArray(buf, options, "", pset.encoding, standard_strings());
                    appendPQExpBufferChar(buf, ')');
                }

                // Add view definition
                appendPQExpBuffer(buf, " AS\n%s", definition);

                // Remove trailing semicolon from pg_get_viewdef
                if (buf->len > 0 && buf->data[buf->len - 1] == ';')
                    buf->data[--(buf->len)] = '\0';

                // Add CHECK OPTION if specified
                if (check_opt && check_opt[0] != '\0')
                    appendPQExpBuffer(buf, "\n WITH %s CHECK OPTION", check_opt);
                break;
        }

        // Ensure result ends with newline
        if (buf->len > 0 && buf->data[buf->len - 1] != '\n')
            appendPQExpBufferChar(buf, '\n');
    } else {
        minimal_error_message(res);
        result = false;
    }

    PQclear(res);
    destroyPQExpBuffer(query);
    return result;
}
```