# describeOneTableDetails

## Location
[src/bin/psql/describe.c:1528-3548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L1528-L3548)

## Overview
The core function that displays detailed information about a single database relation (table, view, index, etc.) for the psql \d command.

## Definition

```c
struct
	{
		int16		checks;
		char		relkind;
		bool		hasindex;
		bool		hasrules;
		bool		hastriggers;
		bool		rowsecurity;
		bool		forcerowsecurity;
		bool		hasoids;
		bool		ispartition;
		Oid			tablespace;
		char	   *reloptions;
		char	   *reloftype;
		char		relpersistence;
		char		relreplident;
		char	   *relam;
	}			tableinfo;
```
## Detailed Description
 is a comprehensive function responsible for displaying detailed information about a single PostgreSQL relation. This 2000+ line function is the heart of psql's \d command functionality, handling the complex task of gathering and formatting information about tables, views, indexes, sequences, and other database objects.

The function performs multiple operations:
1. **General metadata retrieval**: Queries pg_class and related catalogs to get basic relation information (relkind, persistence, options, etc.)
2. **Column information**: Retrieves detailed column data including types, defaults, constraints, collations, and comments
3. **Special handling for sequences**: Provides sequence-specific information display
4. **Indexes and constraints**: Shows index definitions, primary keys, foreign keys, check constraints, and unique constraints
5. **Inheritance and partitioning**: Displays partition information, inheritance hierarchies, and partition constraints
6. **Storage details**: Shows tablespaces, access methods, replication identity, and other storage-related information
7. **Triggers and rules**: Lists associated triggers and rules
8. **Foreign table options**: Shows FDW-specific options for foreign tables

The function adapts its queries and output based on the PostgreSQL server version to ensure compatibility across different releases. It builds the output using the printTable infrastructure to create formatted tabular displays.

## Parameters

- `schemaname`: Schema name of the relation to describe
- `relationname`: Name of the relation to describe  
- `oid`: Object identifier (OID) of the relation as a string
- `verbose`: Boolean flag for verbose mode (\d+ vs \d) - controls display of additional details like column comments, storage information, and statistics targets

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md): Initialize query buffer management
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md): Format SQL queries with parameters
  - [PSQLexec](../P/PSQLexec.md): Execute SQL queries against the database
  - [printTableInit](../p/printTableInit.md): Initialize table formatting structure
  - [printTableAddHeader](../p/printTableAddHeader.md): Add column headers to table output
  - [printTableAddCell](../p/printTableAddCell.md): Add data cells to table output
  - [printTableAddFooter](../p/printTableAddFooter.md): Add footer information to table output
  - [printTable](../p/printTable.md): Display the formatted table
  - [add_tablespace_footer](../a/add_tablespace_footer.md): Add tablespace information to footer
  - atooid: Convert string to OID
  - [fmtId](../f/fmtId.md): Format identifiers for SQL queries
- Called from (representative examples):
  - [describeTableDetails](describeTableDetails.md): For each relation matching the \d pattern

## Notes and Other Information
- The function is marked static, indicating it's only used within the describe.c file
- Handles version-specific SQL queries to maintain compatibility across PostgreSQL versions (9.4+, 10.0+, 12.0+, etc.)
- Uses complex SQL queries with multiple JOINs to gather comprehensive relation information
- Implements special cases for different relation types (RELKIND_SEQUENCE, RELKIND_INDEX, etc.)
- Manages memory carefully with proper cleanup of PQExpBuffer and PGresult structures
- Supports internationalization through gettext_noop for translatable strings
- The function's complexity reflects the rich metadata available in PostgreSQL's system catalogs
- Returns false on any error for proper error propagation to calling functions

## Simplified Source

```c
static bool describeOneTableDetails(const char *schemaname, const char *relationname,
                                   const char *oid, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printTableContent cont;

    // Table metadata structure
    struct {
        int16 checks;
        char relkind;
        bool hasindex;
        bool hasrules;
        bool hastriggers;
        bool rowsecurity;
        bool forcerowsecurity;
        bool hasoids;
        bool ispartition;
        Oid tablespace;
        char *reloptions;
        char *reloftype;
        char relpersistence;
        char relreplident;
        char *relam;
    } tableinfo;

    initPQExpBuffer(&buf);

    // 1. Get basic table metadata
    printfPQExpBuffer(&buf,
        "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, "
        "c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, "
        "c.relhasoids, c.relispartition, c.reltablespace, "
        "CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, "
        "c.relpersistence, c.relreplident, am.amname, "
        "CASE WHEN c.relkind = 'I' THEN pg_catalog.pg_get_indexdef(c.oid) "
        "     ELSE pg_catalog.array_to_string(c.reloptions || "
        "          array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x), ', ') "
        "END "
        "FROM pg_catalog.pg_class c "
        "LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) "
        "LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) "
        "WHERE c.oid = '%s'", oid);

    res = PSQLexec(buf.data);
    if (!res)
        goto error_return;

    // Parse table metadata
    if (PQntuples(res) > 0) {
        tableinfo.checks = atoi(PQgetvalue(res, 0, 0));
        tableinfo.relkind = *(PQgetvalue(res, 0, 1));
        tableinfo.hasindex = strcmp(PQgetvalue(res, 0, 2), "t") == 0;
        tableinfo.hasrules = strcmp(PQgetvalue(res, 0, 3), "t") == 0;
        tableinfo.hastriggers = strcmp(PQgetvalue(res, 0, 4), "t") == 0;
        // ... parse other fields
    }
    PQclear(res);

    // 2. Handle special case for sequences
    if (tableinfo.relkind == RELKIND_SEQUENCE) {
        return describeSequence(schemaname, relationname, oid, verbose);
    }

    // 3. Get column information
    printfPQExpBuffer(&buf,
        "SELECT a.attname, "
        "pg_catalog.format_type(a.atttypid, a.atttypmod), "
        "a.attnotnull, a.atthasdef, a.attidentity, a.attgenerated, "
        "(SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid) FROM pg_catalog.pg_attrdef d "
        " WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), "
        "a.attcollation, a.attlen, a.attalign, a.attstorage "
        "FROM pg_catalog.pg_attribute a "
        "WHERE a.attrelid = '%s' AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum", oid);

    res = PSQLexec(buf.data);
    if (!res)
        goto error_return;

    // 4. Display column information in table format
    printTableInit(&cont, &myopt, "Table", 3, PQntuples(res));
    printTableAddHeader(&cont, "Column", false, false);
    printTableAddHeader(&cont, "Type", false, false);
    printTableAddHeader(&cont, "Collation", false, false);
    if (verbose) {
        printTableAddHeader(&cont, "Nullable", false, false);
        printTableAddHeader(&cont, "Default", false, false);
    }

    // Add column data
    for (int i = 0; i < PQntuples(res); i++) {
        printTableAddCell(&cont, PQgetvalue(res, i, 0), false, false); // Column name
        printTableAddCell(&cont, PQgetvalue(res, i, 1), false, false); // Type
        printTableAddCell(&cont, PQgetvalue(res, i, 6), false, false); // Collation

        if (verbose) {
            printTableAddCell(&cont, PQgetvalue(res, i, 2)[0] == 'f' ? "not null" : "", false, false);
            printTableAddCell(&cont, PQgetvalue(res, i, 5), false, false); // Default
        }
    }
    PQclear(res);

    // 5. Display the table
    printTable(&cont, pset.queryFout, false, pset.logfile);
    printTableCleanup(&cont);

    // 6. Add indexes information if present
    if (tableinfo.hasindex && (tableinfo.relkind == RELKIND_RELATION ||
                               tableinfo.relkind == RELKIND_PARTITIONED_TABLE)) {
        printfPQExpBuffer(&buf,
            "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, "
            "i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), "
            "pg_catalog.pg_get_constraintdef(con.oid, true), "
            "contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace "
            "FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i "
            "LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x')) "
            "WHERE c.oid = '%s' AND c.oid = i.indrelid AND i.indexrelid = c2.oid "
            "ORDER BY i.indisprimary DESC, c2.relname", oid);

        res = PSQLexec(buf.data);
        if (res && PQntuples(res) > 0) {
            printf("Indexes:\n");
            for (int i = 0; i < PQntuples(res); i++) {
                printf("    \"%s\" %s\n", PQgetvalue(res, i, 0), PQgetvalue(res, i, 5));
            }
        }
        if (res) PQclear(res);
    }

    // 7. Add constraints, triggers, rules as needed (simplified)
    if (tableinfo.checks > 0) {
        // Display check constraints
    }
    if (tableinfo.hastriggers) {
        // Display triggers
    }
    if (tableinfo.hasrules) {
        // Display rules
    }

    // 8. Add tablespace and other footer information
    if (tableinfo.tablespace != 0) {
        add_tablespace_footer(&cont, tableinfo.relkind, tableinfo.tablespace, true);
    }

    termPQExpBuffer(&buf);
    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```