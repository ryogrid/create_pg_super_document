# listEventTriggers

## Location
[src/bin/psql/describe.c:4614-4693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4614-L4693)

## Overview
Lists and displays information about event triggers in the PostgreSQL database, corresponding to the psql \dy command.

## Definition
bool listEventTriggers(const char *pattern, bool verbose)

## Detailed Description
The listEventTriggers function generates and executes a SQL query to retrieve information about event triggers from the pg_catalog.pg_event_trigger system catalog. It displays event trigger details including name, event type, owner, enabled status, associated function, and tags. The function first checks if the server version supports event triggers (PostgreSQL 9.3+) and returns early with an error message if not supported. The enabled status is decoded from single-character codes to human-readable strings (enabled, replica, always, disabled). When verbose mode is enabled, it includes descriptions from the system catalog. The function uses array_to_string with unnest to display tags as a comma-separated list.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter event trigger names (can be NULL for no filtering)
- `verbose`: Boolean flag to include extended information (descriptions) in the output

## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - lengthof
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher)

## Notes and Other Information
- Implements the \dy psql meta-command functionality
- Requires PostgreSQL 9.3+ (checks pset.sversion >= 90300)
- Queries the pg_catalog.pg_event_trigger system catalog
- Decodes evtenabled status codes: 'O' = enabled, 'R' = replica, 'A' = always, 'D' = disabled
- Uses pg_catalog.pg_get_userbyid() to resolve owner names
- Converts function OIDs to readable names using regproc cast
- Uses array_to_string with unnest to display tags as comma-separated values
- Uses static translate_columns array to specify which columns should be translated
- Returns true even when server doesn't support event triggers (after showing error)
- Returns false on query validation or execution failure, true on success
- Output is ordered by event trigger name for consistent presentation
- Uses obj_description() in verbose mode to retrieve descriptions from pg_event_trigger catalog

## Simplified Source

```c
bool listEventTriggers(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    static const bool translate_columns[] =
        {false, false, false, true, false, false, false};

    // Check if server supports event triggers (PostgreSQL 9.3+)
    if (pset.sversion < 90300) {
        char sverbuf[32];
        pg_log_error("The server (version %s) does not support event triggers.",
                    formatPGVersionNumber(pset.sversion, false, sverbuf, sizeof(sverbuf)));
        return true;
    }

    initPQExpBuffer(&buf);

    // Build query for event trigger information
    printfPQExpBuffer(&buf,
        "SELECT evtname as \"%s\", "
        "evtevent as \"%s\", "
        "pg_catalog.pg_get_userbyid(e.evtowner) as \"%s\",\n"
        " case evtenabled when 'O' then '%s'"
        "  when 'R' then '%s'"
        "  when 'A' then '%s'"
        "  when 'D' then '%s' end as \"%s\",\n"
        " e.evtfoid::pg_catalog.regproc as \"%s\", "
        "pg_catalog.array_to_string(array(select x"
        " from pg_catalog.unnest(evttags) as t(x)), ', ') as \"%s\"",
        gettext_noop("Name"),
        gettext_noop("Event"),
        gettext_noop("Owner"),
        gettext_noop("enabled"),
        gettext_noop("replica"),
        gettext_noop("always"),
        gettext_noop("disabled"),
        gettext_noop("Enabled"),
        gettext_noop("Function"),
        gettext_noop("Tags"));

    // Add description column if verbose mode requested
    if (verbose)
        appendPQExpBuffer(&buf,
            ",\npg_catalog.obj_description(e.oid, 'pg_event_trigger') as \"%s\"",
            gettext_noop("Description"));

    appendPQExpBufferStr(&buf, "\nFROM pg_catalog.pg_event_trigger e ");

    // Add pattern filtering if specified
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                               NULL, "evtname", NULL, NULL, NULL, 1)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    // Configure output formatting
    myopt.title = _("List of event triggers");
    myopt.translate_header = true;
    myopt.translate_columns = translate_columns;
    myopt.n_translate_columns = lengthof(translate_columns);

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;
}
```