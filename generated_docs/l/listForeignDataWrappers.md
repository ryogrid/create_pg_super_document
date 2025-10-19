# listForeignDataWrappers

## Location
[src/bin/psql/describe.c:5728-5798](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5728-L5798)

## Overview
Lists foreign data wrappers in the PostgreSQL database, showing their names, owners, handlers, and validators, with optional verbose information including access privileges and options.

## Definition
bool listForeignDataWrappers(const char *pattern, bool verbose)

## Detailed Description
This function queries the pg_foreign_data_wrapper system catalog to display information about foreign data wrappers (FDWs) in the database. It shows essential FDW properties including the wrapper name, owner, handler function, and validator function. In verbose mode, it additionally displays access control lists (ACLs), FDW-specific options formatted as key-value pairs, and descriptions. The function supports pattern matching to filter results and provides internationalized column headers. This implements the \dew psql command functionality.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter foreign data wrapper names (can be NULL to show all FDWs)
- `verbose`: Boolean flag to control whether to show additional detailed information (ACLs, options, descriptions)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - gettext_noop
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher)

## Notes and Other Information
- Returns false if pattern validation fails or query execution fails
- In verbose mode, displays FDW options using pg_options_to_table() for proper formatting
- Includes left join with pg_description for object descriptions in verbose mode
- Orders results alphabetically by FDW name for consistent output
- Uses internationalization support for all column headers and titles
- This function corresponds to the \dew command in psql for listing foreign data wrappers

## Simplified Source

```c
bool listForeignDataWrappers(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Initialize buffer and build base query
    initPQExpBuffer(&buf);
    printfPQExpBuffer(&buf,
        "SELECT fdw.fdwname AS \"%s\",\n"
        "  pg_catalog.pg_get_userbyid(fdw.fdwowner) AS \"%s\",\n"
        "  fdw.fdwhandler::pg_catalog.regproc AS \"%s\",\n"
        "  fdw.fdwvalidator::pg_catalog.regproc AS \"%s\"",
        gettext_noop("Name"),
        gettext_noop("Owner"),
        gettext_noop("Handler"),
        gettext_noop("Validator"));

    // Add verbose columns if requested
    if (verbose) {
        appendPQExpBufferStr(&buf, ",\n  ");
        printACLColumn(&buf, "fdwacl");
        appendPQExpBuffer(&buf,
            ",\n CASE WHEN fdwoptions IS NULL THEN '' ELSE "
            "  '(' || pg_catalog.array_to_string(ARRAY(SELECT "
            "  pg_catalog.quote_ident(option_name) ||  ' ' || "
            "  pg_catalog.quote_literal(option_value)  FROM "
            "  pg_catalog.pg_options_to_table(fdwoptions)),  ', ') || ')' "
            "  END AS \"%s\""
            ",\n  d.description AS \"%s\" ",
            gettext_noop("FDW options"),
            gettext_noop("Description"));
    }

    // Add FROM clause and optional description join
    appendPQExpBufferStr(&buf, "\nFROM pg_catalog.pg_foreign_data_wrapper fdw\n");
    if (verbose) {
        appendPQExpBufferStr(&buf,
            "LEFT JOIN pg_catalog.pg_description d\n"
            "       ON d.classoid = fdw.tableoid "
            "AND d.objoid = fdw.oid AND d.objsubid = 0\n");
    }

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                NULL, "fdwname", NULL, NULL,
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

    myopt.title = _("List of foreign-data wrappers");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    return true;
}
```