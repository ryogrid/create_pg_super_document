# describeOneTSConfig

## Location
[src/bin/psql/describe.c:5657-5727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5657-L5727)

## Overview
Displays detailed information about a single text search configuration, including its token types and associated dictionaries.

## Definition
static bool describeOneTSConfig(const char *oid, const char *nspname, const char *cfgname, const char *pnspname, const char *prsname)

## Detailed Description
This function provides a detailed view of a specific text search configuration by querying the pg_ts_config_map catalog table to show the mapping between token types and dictionaries. It constructs a complex SQL query that joins configuration maps with token type information from the parser, displaying which dictionaries are applied to each token type. The output is formatted as a table showing token types in the first column and their corresponding dictionaries in the second column, with proper localization support.

## Parameters / Member Variables
- `oid`: The OID of the text search configuration to describe
- `nspname`: The namespace name of the configuration (can be NULL)
- `cfgname`: The name of the text search configuration
- `pnspname`: The namespace name of the parser (can be NULL)
- `prsname`: The name of the text search parser

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - gettext_noop
- Called from (representative examples):
  - [listTSConfigsVerbose](../l/listTSConfigsVerbose.md)

## Notes and Other Information
- Returns false if the query execution fails
- Formats output with proper titles showing configuration and parser information
- Uses internationalization support for column headers and titles
- The query groups results by configuration, token type, and parser to show comprehensive mapping information
- Output includes both token type aliases and dictionary names for user-friendly display
- This is a static helper function used internally by psql's text search configuration listing functionality

## Simplified Source

```c
static bool describeOneTSConfig(const char *oid, const char *nspname, const char *cfgname,
                               const char *pnspname, const char *prsname) {
    PQExpBufferData buf, title;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Initialize query buffer
    initPQExpBuffer(&buf);

    // Build query for token-to-dictionary mappings
    printfPQExpBuffer(&buf,
        "SELECT "
        "(SELECT t.alias FROM pg_catalog.ts_token_type(c.cfgparser) AS t "
        "WHERE t.tokid = m.maptokentype) AS \"Token\", "
        "pg_catalog.btrim("
        "ARRAY(SELECT mm.mapdict::pg_catalog.regdictionary "
        "FROM pg_catalog.pg_ts_config_map AS mm "
        "WHERE mm.mapcfg = m.mapcfg AND mm.maptokentype = m.maptokentype "
        "ORDER BY mapcfg, maptokentype, mapseqno"
        ")::pg_catalog.text, '{}') AS \"Dictionaries\" "
        "FROM pg_catalog.pg_ts_config AS c, pg_catalog.pg_ts_config_map AS m "
        "WHERE c.oid = '%s' AND m.mapcfg = c.oid "
        "GROUP BY m.mapcfg, m.maptokentype, c.cfgparser "
        "ORDER BY 1;", oid);

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res) return false;

    // Build title with configuration and parser information
    initPQExpBuffer(&title);
    if (nspname) {
        appendPQExpBuffer(&title, "Text search configuration \"%s.%s\"", nspname, cfgname);
    } else {
        appendPQExpBuffer(&title, "Text search configuration \"%s\"", cfgname);
    }

    if (pnspname) {
        appendPQExpBuffer(&title, "\nParser: \"%s.%s\"", pnspname, prsname);
    } else {
        appendPQExpBuffer(&title, "\nParser: \"%s\"", prsname);
    }

    // Configure and display results
    myopt.title = title.data;
    myopt.footers = NULL;
    myopt.topt.default_footer = false;
    myopt.translate_header = true;
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    // Cleanup
    termPQExpBuffer(&title);
    PQclear(res);
    return true;
}
```