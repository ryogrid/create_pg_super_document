# get_rel_infos

## Location
[src/bin/pg_upgrade/info.c:445-639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L445-L639)

## Overview
The get_rel_infos function collects metadata for all user tables, materialized views, toast tables, and indexes within a specific database during the PostgreSQL upgrade process.

## Definition

```c
enumber,
				i_reltablespace;
```
## Detailed Description
This function is a crucial component of pg_upgrade that gathers comprehensive relation metadata from a database. It constructs and executes a complex SQL query using Common Table Expressions (CTEs) to collect information about regular heap tables, toast tables, and indexes. The function categorizes relations into three groups: regular_heap (user tables and materialized views), toast_heap (toast tables for large objects), and all_index (valid indexes). It optimizes memory usage by reusing string allocations for identical namespace and tablespace names. The results are guaranteed to be sorted by OID to enable efficient matching between old and new databases during the upgrade process.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure containing cluster connection and version information
- : Pointer to DbInfo structure representing the specific database being processed

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [pg_strdup](../p/pg_strdup.md)
  - atooid
  - [PQfnumber](../P/PQfnumber.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - CppAsString2
  - strcmp
- Called from (representative examples):
  - [get_db_rel_and_slot_infos](get_db_rel_and_slot_infos.md)

## Notes and Other Information
- Results are sorted by OID to enable efficient old/new database matching
- Uses memory optimization by reusing identical namespace and tablespace string pointers
- Filters relations based on FirstNormalObjectId to exclude system objects
- Handles pg_largeobject specially as it contains user data not in pg_dump output
- Excludes temporary tables, invalid indexes, and system schemas (pg_catalog, information_schema, etc.)
- Only processes valid and ready indexes (indisvalid AND indisready)
- Uses complex CTE-based SQL query to handle different relation types efficiently
- Function is static, indicating internal use within the info.c compilation unit

## Simplified Source

```c
static void
get_rel_infos(ClusterInfo *cluster, DbInfo *dbinfo)
{
    PGconn *connection = connectToServer(cluster, dbinfo->db_name);
    PGresult *query_result;
    RelInfo *relation_infos;
    int num_relations;
    int relation_index;
    char complex_query[QUERY_ALLOC];

    // Build complex CTE-based query to collect all relation types
    // Phase 1: Regular heap tables and materialized views
    snprintf(complex_query, sizeof(complex_query),
        "WITH regular_heap (reloid, indtable, toastheap) AS ( "
        "  SELECT c.oid, 0::oid, 0::oid "
        "  FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
        "         ON c.relnamespace = n.oid "
        "  WHERE relkind IN ('%c', '%c') AND "
        // Exclude system schemas and temp tables, include user objects
        "    ((n.nspname !~ '^pg_temp_' AND "
        "      n.nspname !~ '^pg_toast_temp_' AND "
        "      n.nspname NOT IN ('pg_catalog', 'information_schema', "
        "                        'binary_upgrade', 'pg_toast') AND "
        "      c.oid >= %u::pg_catalog.oid) OR "
        "     (n.nspname = 'pg_catalog' AND "
        "      relname IN ('pg_largeobject') ))), ",
        RELKIND_RELATION, RELKIND_MATVIEW, FirstNormalObjectId);

    // Phase 2: TOAST tables for the regular heap tables
    snprintf(complex_query + strlen(complex_query), sizeof(complex_query) - strlen(complex_query),
        "  toast_heap (reloid, indtable, toastheap) AS ( "
        "  SELECT c.reltoastrelid, 0::oid, c.oid "
        "  FROM regular_heap JOIN pg_catalog.pg_class c "
        "      ON regular_heap.reloid = c.oid "
        "  WHERE c.reltoastrelid != 0), ");

    // Phase 3: Valid indexes on all selected tables
    snprintf(complex_query + strlen(complex_query), sizeof(complex_query) - strlen(complex_query),
        "  all_index (reloid, indtable, toastheap) AS ( "
        "  SELECT indexrelid, indrelid, 0::oid "
        "  FROM pg_catalog.pg_index "
        "  WHERE indisvalid AND indisready "
        "    AND indrelid IN "
        "        (SELECT reloid FROM regular_heap "
        "         UNION ALL "
        "         SELECT reloid FROM toast_heap)) ");

    // Final query: Union all relation types and get their metadata
    snprintf(complex_query + strlen(complex_query), sizeof(complex_query) - strlen(complex_query),
        "SELECT all_rels.*, n.nspname, c.relname, "
        "  c.relfilenode, c.reltablespace, "
        "  pg_catalog.pg_tablespace_location(t.oid) AS spclocation "
        "FROM (SELECT * FROM regular_heap "
        "      UNION ALL "
        "      SELECT * FROM toast_heap "
        "      UNION ALL "
        "      SELECT * FROM all_index) all_rels "
        "  JOIN pg_catalog.pg_class c ON all_rels.reloid = c.oid "
        "  JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
        "  LEFT OUTER JOIN pg_catalog.pg_tablespace t ON c.reltablespace = t.oid "
        "ORDER BY 1;");

    // Execute the complex query
    query_result = executeQueryOrDie(connection, "%s", complex_query);
    num_relations = PQntuples(query_result);
    relation_infos = pg_malloc(sizeof(RelInfo) * num_relations);

    // Extract column indices for result parsing
    int reloid_idx = PQfnumber(query_result, "reloid");
    int indtable_idx = PQfnumber(query_result, "indtable");
    int toastheap_idx = PQfnumber(query_result, "toastheap");
    int nspname_idx = PQfnumber(query_result, "nspname");
    int relname_idx = PQfnumber(query_result, "relname");
    int relfilenode_idx = PQfnumber(query_result, "relfilenode");
    int reltablespace_idx = PQfnumber(query_result, "reltablespace");
    int spclocation_idx = PQfnumber(query_result, "spclocation");

    // Parse each relation and populate RelInfo structures
    char *last_namespace = NULL, *last_tablespace = NULL;
    for (relation_index = 0; relation_index < num_relations; relation_index++)
    {
        RelInfo *current_rel = &relation_infos[relation_index];

        // Parse basic relation identifiers
        current_rel->reloid = atooid(PQgetvalue(query_result, relation_index, reloid_idx));
        current_rel->indtable = atooid(PQgetvalue(query_result, relation_index, indtable_idx));
        current_rel->toastheap = atooid(PQgetvalue(query_result, relation_index, toastheap_idx));

        // Optimize memory usage by reusing identical namespace strings
        char *namespace_name = PQgetvalue(query_result, relation_index, nspname_idx);
        if (last_namespace && strcmp(namespace_name, last_namespace) == 0)
            current_rel->nspname = last_namespace;
        else
            last_namespace = current_rel->nspname = pg_strdup(namespace_name);

        // Parse relation name and file information
        current_rel->relname = pg_strdup(PQgetvalue(query_result, relation_index, relname_idx));
        current_rel->relfilenumber = atooid(PQgetvalue(query_result, relation_index, relfilenode_idx));

        // Handle tablespace information (optimize memory for identical tablespaces)
        if (atooid(PQgetvalue(query_result, relation_index, reltablespace_idx)) != 0)
        {
            char *tablespace_location = PQgetvalue(query_result, relation_index, spclocation_idx);
            if (last_tablespace && strcmp(tablespace_location, last_tablespace) == 0)
                current_rel->tablespace = last_tablespace;
            else
                last_tablespace = current_rel->tablespace = pg_strdup(tablespace_location);
        }
        else
            current_rel->tablespace = dbinfo->db_tablespace;
    }

    PQclear(query_result);
    PQfinish(connection);

    // Store results in database information structure
    dbinfo->rel_arr.rels = relation_infos;
    dbinfo->rel_arr.nrels = num_relations;
}
```