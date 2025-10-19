# describeOneTSParser

## Location
[src/bin/psql/describe.c:5274-5393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5274-L5393)

## Overview
Displays detailed information about a specific text search parser, including its constituent functions and supported token types.

## Definition
static bool describeOneTSParser(const char *oid, const char *nspname, const char *prsname)

## Detailed Description
This function provides comprehensive details about a single text search parser by executing two separate queries. The first query retrieves information about the parser's five core functions (start parse, get next token, end parse, get headline, and get token types) along with their descriptions. The second query calls the ts_token_type function to display all token types supported by the parser. Results are formatted and displayed using psql's table formatting system with appropriate titles and column translations.

## Parameters / Member Variables
- `oid`: Object identifier of the text search parser in pg_ts_parser catalog
- `nspname`: Namespace name of the parser (may be NULL for parsers in current search path)
- `prsname`: Name of the text search parser

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)  
  - [PSQLexec](../P/PSQLexec.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - gettext_noop
  - lengthof
- Called from (representative examples):
  - [listTSParsersVerbose](../l/listTSParsersVerbose.md)

## Notes and Other Information
- Returns false on error, true on success
- Uses UNION ALL queries to display parser function information in a structured format
- Implements internationalization through gettext_noop for column headers
- Displays two separate result tables: parser functions and token types
- Part of psql's \dFp+ command implementation for detailed parser inspection
- Handles both schema-qualified and unqualified parser names in display titles

## Simplified Source

```c
static bool describeOneTSParser(const char *oid, const char *nspname, const char *prsname) {
    PQExpBufferData buf, title;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    static const bool translate_columns[] = {true, false, false};

    // Initialize buffers
    initPQExpBuffer(&buf);

    // Build query for parser functions using UNION ALL
    printfPQExpBuffer(&buf,
        "SELECT 'Start parse' AS \"Method\", "
        "p.prsstart::pg_catalog.regproc AS \"Function\", "
        "pg_catalog.obj_description(p.prsstart, 'pg_proc') AS \"Description\" "
        "FROM pg_catalog.pg_ts_parser p WHERE p.oid = '%s' "
        "UNION ALL "
        "SELECT 'Get next token', p.prstoken::pg_catalog.regproc, "
        "pg_catalog.obj_description(p.prstoken, 'pg_proc') "
        "FROM pg_catalog.pg_ts_parser p WHERE p.oid = '%s' "
        "UNION ALL "
        "SELECT 'End parse', p.prsend::pg_catalog.regproc, "
        "pg_catalog.obj_description(p.prsend, 'pg_proc') "
        "FROM pg_catalog.pg_ts_parser p WHERE p.oid = '%s' "
        "UNION ALL "
        "SELECT 'Get headline', p.prsheadline::pg_catalog.regproc, "
        "pg_catalog.obj_description(p.prsheadline, 'pg_proc') "
        "FROM pg_catalog.pg_ts_parser p WHERE p.oid = '%s' "
        "UNION ALL "
        "SELECT 'Get token types', p.prslextype::pg_catalog.regproc, "
        "pg_catalog.obj_description(p.prslextype, 'pg_proc') "
        "FROM pg_catalog.pg_ts_parser p WHERE p.oid = '%s';",
        oid, oid, oid, oid, oid);

    // Execute parser functions query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res) return false;

    // Configure title for parser functions
    initPQExpBuffer(&title);
    if (nspname) {
        printfPQExpBuffer(&title, "Text search parser \"%s.%s\"", nspname, prsname);
    } else {
        printfPQExpBuffer(&title, "Text search parser \"%s\"", prsname);
    }

    // Display parser functions
    myopt.title = title.data;
    myopt.translate_header = true;
    myopt.translate_columns = translate_columns;
    myopt.n_translate_columns = lengthof(translate_columns);
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    // Build query for token types
    initPQExpBuffer(&buf);
    printfPQExpBuffer(&buf,
        "SELECT t.alias AS \"Token name\", t.description AS \"Description\" "
        "FROM pg_catalog.ts_token_type('%s'::pg_catalog.oid) AS t "
        "ORDER BY 1;", oid);

    // Execute token types query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res) {
        termPQExpBuffer(&title);
        return false;
    }

    // Configure title for token types
    if (nspname) {
        printfPQExpBuffer(&title, "Token types for parser \"%s.%s\"", nspname, prsname);
    } else {
        printfPQExpBuffer(&title, "Token types for parser \"%s\"", prsname);
    }

    // Display token types
    myopt.title = title.data;
    myopt.translate_columns = NULL;
    myopt.n_translate_columns = 0;
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    // Cleanup
    termPQExpBuffer(&title);
    PQclear(res);
    return true;
}
```