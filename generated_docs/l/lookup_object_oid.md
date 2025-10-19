# lookup_object_oid

## Location
[src/bin/psql/command.c:5606-5665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5606-L5665)

## Overview
Looks up PostgreSQL database objects by their textual description and retrieves their internal OID (Object Identifier) for further processing.

## Definition

```c
static bool
lookup_object_oid(EditableObjectType obj_type, const char *desc,
				  Oid *obj_oid)
```
## Detailed Description
The  function provides a unified interface for converting textual object descriptions into PostgreSQL internal Object Identifiers (OIDs). It supports different object types with type-specific lookup strategies using PostgreSQL's built-in conversion functions like regproc, regprocedure, and regclass.

For functions, it constructs queries using regproc (for simple function names) or regprocedure (for function signatures with parentheses) to handle overloading. For views and relations, it uses regclass conversion. The function integrates with psql's ECHO_HIDDEN system to optionally display the lookup queries and handles various error conditions including non-existent objects, multiple matches, and syntax errors in object descriptions.

## Parameters / Member Variables
- `obj_type`: EditableObjectType enum specifying the type of object to lookup (EditableFunction, EditableView)
- `*desc`: String description of the object (e.g., function name with or without signature, view name)
- `*obj_oid`: Output parameter to store the retrieved OID upon successful lookup
## Dependencies
- Functions called/Symbols referenced:
  - EditableObjectType (enum defining supported object types for editing)
  - EditableFunction, EditableView (enum values for different object types)
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md) (safely quotes string literals for SQL queries)
  - [echo_hidden_command](../e/echo_hidden_command.md) (displays query if ECHO_HIDDEN is enabled)
  - [PQexec](../P/PQexec.md) (executes the lookup query against the database)
  - PGRES_TUPLES_OK (PostgreSQL result status indicating successful tuple retrieval)
  - atooid (converts string representation to OID)
  - [minimal_error_message](../m/minimal_error_message.md) (displays error information for failed queries)
- Called from (representative examples):
  - [exec_command_ef_ev](../e/exec_command_ef_ev.md) (handles \ef and \ev commands for editing functions/views)
  - [exec_command_sf_sv](../e/exec_command_sf_sv.md) (handles \sf and \sv commands for showing function/view definitions)

## Notes and Other Information
- Fails for non-existent objects, multiple matching candidates, or syntactically invalid object descriptions
- Uses PostgreSQL's type conversion system (regproc/regprocedure/regclass) for robust object resolution
- Automatically detects function signatures by checking for parentheses in the description
- Integrates with psql's ECHO_HIDDEN functionality for query transparency
- Part of psql's object editing and inspection infrastructure
- Does not validate object types beyond basic existence - type validation occurs in downstream functions
- Essential for psql commands that need to operate on specific database objects by name

## Simplified Source

```c
static bool lookup_object_oid(EditableObjectType obj_type, const char *desc,
                             Oid *obj_oid) {
    bool result = true;
    PQExpBuffer query = createPQExpBuffer();
    PGresult *res;

    // Build type-specific lookup query
    switch (obj_type) {
        case EditableFunction:
            // Use regproc for simple names, regprocedure for signatures with parentheses
            appendPQExpBufferStr(query, "SELECT ");
            appendStringLiteralConn(query, desc, pset.db);
            appendPQExpBuffer(query, "::pg_catalog.%s::pg_catalog.oid",
                            strchr(desc, '(') ? "regprocedure" : "regproc");
            break;

        case EditableView:
            // Convert view/relation name to OID using regclass
            appendPQExpBufferStr(query, "SELECT ");
            appendStringLiteralConn(query, desc, pset.db);
            appendPQExpBufferStr(query, "::pg_catalog.regclass::pg_catalog.oid");
            break;
    }

    // Execute lookup query
    if (!echo_hidden_command(query->data)) {
        destroyPQExpBuffer(query);
        return false;
    }

    res = PQexec(pset.db, query->data);

    // Check result and extract OID
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1) {
        *obj_oid = atooid(PQgetvalue(res, 0, 0));
    } else {
        minimal_error_message(res);
        result = false;
    }

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(query);

    return result;
}
```