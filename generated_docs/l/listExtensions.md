# listExtensions

## Location
[src/bin/psql/describe.c:6002-6052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6002-L6052)

## Overview
Implements the  command in psql to display a brief list of installed PostgreSQL extensions with their names, versions, schemas, and descriptions.

## Definition

```c
bool
listExtensions(const char *pattern)
```
## Detailed Description
This function queries the PostgreSQL system catalogs to retrieve information about installed extensions. It constructs a SQL query that joins the pg_extension catalog with pg_namespace and pg_description to provide comprehensive extension information. The function supports pattern matching for selective display of extensions and presents the results in a formatted table showing extension name, version, schema, and description.

The query retrieves:
- Extension name
- Extension version
- Schema name where the extension is installed
- Extension description (from pg_description)

## Parameters / Member Variables
- `*pattern`: SQL name pattern for filtering extensions (can be NULL for all extensions)
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (data structure)
  - [printQueryOpt](../p/printQueryOpt.md) (data structure)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in src/bin/psql/command.c:1014)

## Notes and Other Information
- This function is part of psql's describe commands (\d family)
- Uses internationalization with gettext_noop for column headers
- Implements proper error handling by returning false on failures
- The query uses LEFT JOINs to handle extensions that might not have descriptions or might be in unusual schemas
- Pattern validation is handled by validateSQLNamePattern to ensure SQL injection safety
- Results are ordered by extension name for consistent presentation
- Unlike the more detailed listExtensionContents, this provides a summary view of all extensions

## Simplified Source

```c
bool listExtensions(const char *pattern) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Initialize buffer and build query
    initPQExpBuffer(&buf);
    printfPQExpBuffer(&buf,
        "SELECT e.extname AS \"%s\", "
        "e.extversion AS \"%s\", n.nspname AS \"%s\", c.description AS \"%s\"\n"
        "FROM pg_catalog.pg_extension e "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace "
        "LEFT JOIN pg_catalog.pg_description c ON c.objoid = e.oid "
        "AND c.classoid = 'pg_catalog.pg_extension'::pg_catalog.regclass\n",
        gettext_noop("Name"),
        gettext_noop("Version"),
        gettext_noop("Schema"),
        gettext_noop("Description"));

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern,
                                false, false,
                                NULL, "e.extname", NULL,
                                NULL,
                                NULL, 1)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of installed extensions");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    return true;
}
```