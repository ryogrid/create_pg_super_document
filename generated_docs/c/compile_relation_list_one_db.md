# compile_relation_list_one_db

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1883-2223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1883-L2223)

## Overview
Compiles a list of relations (tables and indexes) to check within the currently connected database based on user-supplied options, sorted by descending size, and appends them to the given list of relations.

## Definition

```c
static void
compile_relation_list_one_db(PGconn *conn, SimplePtrList *relations,
							 const DatabaseInfo *dat,
							 uint64 *pagecount)
```
## Detailed Description
This function is a core component of the PostgreSQL  utility that builds a comprehensive list of database relations to be checked by the amcheck extension. The function constructs a complex SQL query using Common Table Expressions (CTEs) to identify relations based on inclusion/exclusion patterns and various filtering options.

The function handles several key aspects:
- **Pattern Matching**: Processes inclusion and exclusion patterns for relation selection using regular expressions for database, namespace, and relation names
- **Relation Types**: Supports both heap tables (relam = HEAP_TABLE_AM_OID) and btree indexes (relam = BTREE_AM_OID)  
- **Dependent Objects**: Optionally includes associated toast tables and btree indexes based on command-line options (--no-dependent-toast, --no-dependent-indexes)
- **Sorting**: Orders results by descending page count to process larger relations first
- **Block Range Support**: Calculates actual blocks to check when --startblock/--endblock options are specified for heap tables

The constructed SQL query uses multiple CTEs:
- /: Processes inclusion patterns
- /: Processes exclusion patterns  
- : Main relation selection CTE
- : Toast table selection (if enabled)
- : Btree index selection (if enabled)
- : Toast table index selection (if enabled)

## Parameters / Member Variables
- `*conn`: Active PostgreSQL connection to the database being processed
- `*relations`: SimplePtrList to which discovered RelationInfo structures are appended
- `*dat`: DatabaseInfo structure containing connection details and amcheck extension information for the relations
- `*pagecount`: Pointer to uint64 counter that gets incremented by the total number of blocks to check across all added relations
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [append_rel_pattern_raw_cte](../a/append_rel_pattern_raw_cte.md)
  - [append_rel_pattern_filtered_cte](../a/append_rel_pattern_filtered_cte.md)
  - [executeQuery](../e/executeQuery.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - atooid
  - [pg_malloc0](../p/pg_malloc0.md)
  - [simple_ptr_list_append](../s/simple_ptr_list_append.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [disconnectDatabase](../d/disconnectDatabase.md)
- Called from:
  - [main](../m/main.md) (src/bin/pg_amcheck/pg_amcheck.c:634)

## Notes and Other Information
- This function is specific to the  utility and operates within a single database context
- The function excludes temporary relations (relpersistence != 't') as they belong to other sessions
- [Complex](../C/Complex.md) logic handles the interaction between --allrel mode and specific inclusion patterns to avoid duplicate selection of dependent objects
- Error handling includes detailed query logging when SQL execution fails
- The function supports progress tracking by calculating expected block counts for heap table range checking
- Results are deduplicated using UNION operations to handle cases where relations match multiple patterns or appear in multiple CTEs

## Simplified Source

```c
static void compile_relation_list_one_db(PGconn *conn, SimplePtrList *relations,
                                         const DatabaseInfo *dat, uint64 *pagecount) {
    PQExpBufferData sql;
    initPQExpBuffer(&sql);
    appendPQExpBufferStr(&sql, "WITH");

    // Add inclusion pattern CTEs if not --allrel
    if (!opts.allrel) {
        appendPQExpBufferStr(&sql,
            " include_raw (pattern_id, db_regex, nsp_regex, rel_regex, heap_only, btree_only) AS (");
        append_rel_pattern_raw_cte(&sql, &opts.include, conn);
        appendPQExpBufferStr(&sql, "\n),");
        append_rel_pattern_filtered_cte(&sql, "include_raw", "include_pat", conn);
    }

    // Add exclusion pattern CTEs if needed
    if (opts.excludetbl || opts.excludeidx || opts.excludensp) {
        appendPQExpBufferStr(&sql,
            " exclude_raw (pattern_id, db_regex, nsp_regex, rel_regex, heap_only, btree_only) AS (");
        append_rel_pattern_raw_cte(&sql, &opts.exclude, conn);
        appendPQExpBufferStr(&sql, "\n),");
        append_rel_pattern_filtered_cte(&sql, "exclude_raw", "exclude_pat", conn);
    }

    // Build main relation CTE
    appendPQExpBufferStr(&sql,
        " relation (pattern_id, oid, nspname, relname, reltoastrelid, relpages, is_heap, is_btree) AS ("
        "\nSELECT DISTINCT ON (c.oid");

    if (!opts.allrel)
        appendPQExpBufferStr(&sql, ", ip.pattern_id) ip.pattern_id,");
    else
        appendPQExpBufferStr(&sql, ") NULL::INTEGER AS pattern_id,");

    // Core relation selection query
    appendPQExpBuffer(&sql,
        "\nc.oid, n.nspname, c.relname, c.reltoastrelid, c.relpages, "
        "c.relam = %u AS is_heap, c.relam = %u AS is_btree"
        "\nFROM pg_catalog.pg_class c "
        "INNER JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid",
        HEAP_TABLE_AM_OID, BTREE_AM_OID);

    // Add inclusion pattern joins if not --allrel
    if (!opts.allrel)
        appendPQExpBuffer(&sql,
            "\nINNER JOIN include_pat ip"
            "\nON (n.nspname ~ ip.nsp_regex OR ip.nsp_regex IS NULL)"
            "\nAND (c.relname ~ ip.rel_regex OR ip.rel_regex IS NULL)"
            "\nAND (c.relam = %u OR NOT ip.heap_only)"
            "\nAND (c.relam = %u OR NOT ip.btree_only)",
            HEAP_TABLE_AM_OID, BTREE_AM_OID);

    // Add exclusion pattern joins if needed
    if (opts.excludetbl || opts.excludeidx || opts.excludensp)
        appendPQExpBuffer(&sql,
            "\nLEFT OUTER JOIN exclude_pat ep"
            "\nON (n.nspname ~ ep.nsp_regex OR ep.nsp_regex IS NULL)"
            "\nAND (c.relname ~ ep.rel_regex OR ep.rel_regex IS NULL)"
            "\nAND (c.relam = %u OR NOT ep.heap_only OR ep.rel_regex IS NULL)"
            "\nAND (c.relam = %u OR NOT ep.btree_only OR ep.rel_regex IS NULL)",
            HEAP_TABLE_AM_OID, BTREE_AM_OID);

    // Add WHERE conditions
    appendPQExpBufferStr(&sql, "\nWHERE c.relpersistence != 't'"); // Exclude temp tables
    if (opts.excludetbl || opts.excludeidx || opts.excludensp)
        appendPQExpBufferStr(&sql, "\nAND ep.pattern_id IS NULL");

    // Filter relation types based on --allrel
    if (opts.allrel)
        appendPQExpBuffer(&sql,
            " AND c.relam = %u AND c.relkind IN ('r', 'S', 'm', 't') "
            "AND c.relnamespace != %u",
            HEAP_TABLE_AM_OID, PG_TOAST_NAMESPACE);
    else
        appendPQExpBuffer(&sql,
            " AND c.relam IN (%u, %u) AND c.relkind IN ('r', 'S', 'm', 't', 'i') "
            "AND ((c.relam = %u AND c.relkind IN ('r', 'S', 'm', 't')) OR "
            "(c.relam = %u AND c.relkind = 'i'))",
            HEAP_TABLE_AM_OID, BTREE_AM_OID, HEAP_TABLE_AM_OID, BTREE_AM_OID);

    appendPQExpBufferStr(&sql, "\nORDER BY c.oid)");

    // Add toast table CTE if enabled
    if (!opts.no_toast_expansion) {
        appendPQExpBufferStr(&sql,
            ", toast (oid, nspname, relname, relpages) AS ("
            "\nSELECT t.oid, 'pg_toast', t.relname, t.relpages"
            "\nFROM pg_catalog.pg_class t INNER JOIN relation r ON r.reltoastrelid = t.oid");
        if (opts.excludetbl || opts.excludensp)
            appendPQExpBufferStr(&sql,
                "\nLEFT OUTER JOIN exclude_pat ep"
                "\nON ('pg_toast' ~ ep.nsp_regex OR ep.nsp_regex IS NULL)"
                "\nAND (t.relname ~ ep.rel_regex OR ep.rel_regex IS NULL)"
                "\nAND ep.heap_only WHERE ep.pattern_id IS NULL"
                "\nAND t.relpersistence != 't'");
        appendPQExpBufferStr(&sql, "\n)");
    }

    // Add btree index CTE if enabled
    if (!opts.no_btree_expansion) {
        appendPQExpBufferStr(&sql,
            ", index (oid, nspname, relname, relpages) AS ("
            "\nSELECT c.oid, r.nspname, c.relname, c.relpages FROM relation r"
            "\nINNER JOIN pg_catalog.pg_index i ON r.oid = i.indrelid "
            "INNER JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid "
            "AND c.relpersistence != 't'");

        if (opts.excludeidx || opts.excludensp)
            appendPQExpBufferStr(&sql,
                "\nINNER JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid"
                "\nLEFT OUTER JOIN exclude_pat ep "
                "ON (n.nspname ~ ep.nsp_regex OR ep.nsp_regex IS NULL) "
                "AND (c.relname ~ ep.rel_regex OR ep.rel_regex IS NULL) "
                "AND ep.btree_only WHERE ep.pattern_id IS NULL");
        else
            appendPQExpBufferStr(&sql, "\nWHERE true");

        appendPQExpBuffer(&sql, " AND c.relam = %u AND c.relkind = 'i'", BTREE_AM_OID);
        if (opts.no_toast_expansion)
            appendPQExpBuffer(&sql, " AND c.relnamespace != %u", PG_TOAST_NAMESPACE);
        appendPQExpBufferStr(&sql, "\n)");
    }

    // Add toast index CTE if both toast and btree expansion enabled
    if (!opts.no_toast_expansion && !opts.no_btree_expansion) {
        appendPQExpBufferStr(&sql,
            ", toast_index (oid, nspname, relname, relpages) AS ("
            "\nSELECT c.oid, 'pg_toast', c.relname, c.relpages FROM toast t "
            "INNER JOIN pg_catalog.pg_index i ON t.oid = i.indrelid"
            "\nINNER JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid "
            "AND c.relpersistence != 't'");

        if (opts.excludeidx)
            appendPQExpBufferStr(&sql,
                "\nLEFT OUTER JOIN exclude_pat ep "
                "ON ('pg_toast' ~ ep.nsp_regex OR ep.nsp_regex IS NULL) "
                "AND (c.relname ~ ep.rel_regex OR ep.rel_regex IS NULL) "
                "AND ep.btree_only WHERE ep.pattern_id IS NULL");
        else
            appendPQExpBufferStr(&sql, "\nWHERE true");

        appendPQExpBuffer(&sql, " AND c.relam = %u AND c.relkind = 'i')", BTREE_AM_OID);
    }

    // Build final UNION query
    appendPQExpBufferStr(&sql,
        "\nSELECT pattern_id, is_heap, is_btree, oid, nspname, relname, relpages FROM ("
        "\nSELECT pattern_id, is_heap, is_btree, NULL::OID AS oid, "
        "NULL::TEXT AS nspname, NULL::TEXT AS relname, NULL::INTEGER AS relpages"
        "\nFROM relation WHERE pattern_id IS NOT NULL UNION"
        "\nSELECT NULL::INTEGER AS pattern_id, is_heap, is_btree, oid, nspname, relname, relpages "
        "FROM relation");

    if (!opts.no_toast_expansion)
        appendPQExpBufferStr(&sql, " UNION"
            "\nSELECT NULL::INTEGER AS pattern_id, TRUE AS is_heap, "
            "FALSE AS is_btree, oid, nspname, relname, relpages FROM toast");

    if (!opts.no_btree_expansion)
        appendPQExpBufferStr(&sql, " UNION"
            "\nSELECT NULL::INTEGER AS pattern_id, FALSE AS is_heap, "
            "TRUE AS is_btree, oid, nspname, relname, relpages FROM index");

    if (!opts.no_toast_expansion && !opts.no_btree_expansion)
        appendPQExpBufferStr(&sql, " UNION"
            "\nSELECT NULL::INTEGER AS pattern_id, FALSE AS is_heap, "
            "TRUE AS is_btree, oid, nspname, relname, relpages FROM toast_index");

    appendPQExpBufferStr(&sql, "\n) AS combined_records ORDER BY relpages DESC NULLS FIRST, oid");

    // Execute query and process results
    PGresult *res = executeQuery(conn, sql.data, opts.echo);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log_error("query failed: %s", PQerrorMessage(conn));
        pg_log_error_detail("Query was: %s", sql.data);
        disconnectDatabase(conn);
        exit(1);
    }
    termPQExpBuffer(&sql);

    // Process each result row
    int ntups = PQntuples(res);
    for (int i = 0; i < ntups; i++) {
        int pattern_id = -1;
        bool is_heap = false, is_btree = false;
        Oid oid = InvalidOid;
        const char *nspname = NULL, *relname = NULL;
        int relpages = 0;

        // Extract values from result row
        if (!PQgetisnull(res, i, 0)) pattern_id = atoi(PQgetvalue(res, i, 0));
        if (!PQgetisnull(res, i, 1)) is_heap = (PQgetvalue(res, i, 1)[0] == 't');
        if (!PQgetisnull(res, i, 2)) is_btree = (PQgetvalue(res, i, 2)[0] == 't');
        if (!PQgetisnull(res, i, 3)) oid = atooid(PQgetvalue(res, i, 3));
        if (!PQgetisnull(res, i, 4)) nspname = PQgetvalue(res, i, 4);
        if (!PQgetisnull(res, i, 5)) relname = PQgetvalue(res, i, 5);
        if (!PQgetisnull(res, i, 6)) relpages = atoi(PQgetvalue(res, i, 6));

        if (pattern_id >= 0) {
            // Record that inclusion pattern matched
            opts.include.data[pattern_id].matched = true;
        } else {
            // Create RelationInfo for this relation
            RelationInfo *rel = (RelationInfo *) pg_malloc0(sizeof(RelationInfo));
            rel->datinfo = dat;
            rel->reloid = oid;
            rel->is_heap = is_heap;
            rel->nspname = pstrdup(nspname);
            rel->relname = pstrdup(relname);
            rel->relpages = relpages;
            rel->blocks_to_check = relpages;

            // Calculate actual blocks to check for heap tables with start/end block limits
            if (is_heap && (opts.startblock >= 0 || opts.endblock >= 0)) {
                if (opts.endblock >= 0 && rel->blocks_to_check > opts.endblock)
                    rel->blocks_to_check = opts.endblock + 1;
                if (opts.startblock >= 0) {
                    if (rel->blocks_to_check > opts.startblock)
                        rel->blocks_to_check -= opts.startblock;
                    else
                        rel->blocks_to_check = 0;
                }
            }

            *pagecount += rel->blocks_to_check;
            simple_ptr_list_append(relations, rel);
        }
    }
    PQclear(res);
}
```