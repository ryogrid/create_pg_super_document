# get_db_infos

## Location
[src/bin/pg_upgrade/info.c:379-444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L379-L444)

## Overview
The get_db_infos function scans the pg_database system catalog and populates database information for all user databases within a PostgreSQL cluster during the upgrade process.

## Definition

```c
enumber,
				i_reltablespace;
```
## Detailed Description
This function is a core component of PostgreSQL's pg_upgrade utility that collects essential database metadata from the source cluster. It connects to the 'template1' database and executes a SQL query to retrieve database information including OID, name, encoding, collation settings, and tablespace location. The function handles version-specific differences in locale provider information across different PostgreSQL major versions (15.0+, 17.0+). The collected data is stored in the cluster's database array (dbarr) for use throughout the upgrade process.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure containing cluster connection and metadata information

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - GET_MAJOR_VERSION
  - [pg_malloc0](../p/pg_malloc0.md)
  - atooid
  - [pg_strdup](../p/pg_strdup.md)
  - [PQfinish](../P/PQfinish.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [get_db_rel_and_slot_infos](get_db_rel_and_slot_infos.md)

## Notes and Other Information
- Only collects information for databases where datallowconn is true (databases that allow connections)
- Handles version-specific SQL query construction for locale provider information
- Results are ordered by database OID
- Memory allocation uses pg_malloc0 for zero-initialized DbInfo array
- Connection is made specifically to template1 database for system catalog access
- Function is static, indicating internal use within the info.c compilation unit

## Simplified Source

```c
static void
get_db_infos(ClusterInfo *cluster)
{
    PGconn *connection = connectToServer(cluster, "template1");
    PGresult *query_result;
    int num_databases;
    int row_index;
    DbInfo *database_infos;
    char sql_query[QUERY_ALLOC];

    // Build version-specific SQL query for database information
    snprintf(sql_query, sizeof(sql_query),
             "SELECT d.oid, d.datname, d.encoding, d.datcollate, d.datctype, ");

    // Add version-specific locale provider fields
    if (GET_MAJOR_VERSION(cluster->major_version) >= 1700)
        snprintf(sql_query + strlen(sql_query), sizeof(sql_query) - strlen(sql_query),
                 "datlocprovider, datlocale, ");
    else if (GET_MAJOR_VERSION(cluster->major_version) >= 1500)
        snprintf(sql_query + strlen(sql_query), sizeof(sql_query) - strlen(sql_query),
                 "datlocprovider, daticulocale AS datlocale, ");
    else
        snprintf(sql_query + strlen(sql_query), sizeof(sql_query) - strlen(sql_query),
                 "'c' AS datlocprovider, NULL AS datlocale, ");

    // Complete the query with tablespace location and filters
    snprintf(sql_query + strlen(sql_query), sizeof(sql_query) - strlen(sql_query),
             "pg_catalog.pg_tablespace_location(t.oid) AS spclocation "
             "FROM pg_catalog.pg_database d "
             " LEFT OUTER JOIN pg_catalog.pg_tablespace t "
             " ON d.dattablespace = t.oid "
             "WHERE d.datallowconn = true "
             "ORDER BY 1");

    // Execute query and process results
    query_result = executeQueryOrDie(connection, "%s", sql_query);

    // Extract column indices for result parsing
    int oid_idx = PQfnumber(query_result, "oid");
    int name_idx = PQfnumber(query_result, "datname");
    int tablespace_idx = PQfnumber(query_result, "spclocation");

    // Allocate memory for database information array
    num_databases = PQntuples(query_result);
    database_infos = pg_malloc0(sizeof(DbInfo) * num_databases);

    // Parse each database row and populate DbInfo structures
    for (row_index = 0; row_index < num_databases; row_index++)
    {
        database_infos[row_index].db_oid = atooid(PQgetvalue(query_result, row_index, oid_idx));
        database_infos[row_index].db_name = pg_strdup(PQgetvalue(query_result, row_index, name_idx));
        snprintf(database_infos[row_index].db_tablespace,
                 sizeof(database_infos[row_index].db_tablespace), "%s",
                 PQgetvalue(query_result, row_index, tablespace_idx));
    }

    PQclear(query_result);
    PQfinish(connection);

    // Store results in cluster structure
    cluster->dbarr.dbs = database_infos;
    cluster->dbarr.ndbs = num_databases;
}
```