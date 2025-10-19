# get_loadable_libraries

## Location
[src/bin/pg_upgrade/function.c:55-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/function.c#L55-L145)

## Overview
Fetches the names of all libraries containing C-language functions and logical replication output plugins from the old PostgreSQL cluster during pg_upgrade operations.

## Definition
```c
void get_loadable_libraries(void)
```

## Detailed Description
This function systematically collects library names from all databases in the old PostgreSQL cluster to ensure they can be verified in the new installation during upgrade. It performs two main collection tasks: first, it queries each database for libraries containing non-built-in C functions by examining pg_proc entries where prolang matches ClanguageId and probin is not NULL. Second, it includes logical replication output plugin names from active replication slots. The collected library information is stored in the global os_info.libraries array along with associated database numbers for later verification by check_loadable_libraries().

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
  - [connectToServer](../c/connectToServer.md) (database connection)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL query execution)
  - [count_old_cluster_logical_slots](../c/count_old_cluster_logical_slots.md) (logical slot counting)
  - [PQntuples](../P/PQntuples.md), PQgetvalue, PQclear, PQfinish (PostgreSQL result handling)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
  - [pg_free](../p/pg_free.md) (memory deallocation)
  - [DbInfo](../D/DbInfo.md), LibraryInfo, LogicalSlotInfoArr (structure types)
  - FirstNormalObjectId, ClanguageId (constants)
- Called from (representative examples):
  - [check_and_dump_old_cluster](../c/check_and_dump_old_cluster.md)

## Notes and Other Information
- Modifies global os_info.libraries and os_info.num_libraries
- Removes duplicate library names within each database automatically via DISTINCT query
- Does not eliminate duplicates across different databases or between C functions and logical replication plugins
- Memory allocated for libraries must be freed elsewhere in the program
- Uses FirstNormalObjectId to exclude built-in system functions
- Handles invalid replication slots by skipping them during plugin collection

## Simplified Source

```c
void
get_loadable_libraries(void)
{
    PGresult **query_results;
    int total_libraries = 0;
    int db_index;

    // Allocate array to store query results from each database
    query_results = pg_malloc(old_cluster.dbarr.ndbs * sizeof(PGresult *));

    // Phase 1: Query each database for C function libraries
    for (db_index = 0; db_index < old_cluster.dbarr.ndbs; db_index++)
    {
        DbInfo *current_db = &old_cluster.dbarr.dbs[db_index];
        PGconn *connection = connectToServer(&old_cluster, current_db->db_name);

        // Get all distinct libraries containing non-built-in C functions
        query_results[db_index] = executeQueryOrDie(connection,
            "SELECT DISTINCT probin "
            "FROM pg_catalog.pg_proc "
            "WHERE prolang = %u AND probin IS NOT NULL AND oid >= %u;",
            ClanguageId, FirstNormalObjectId);

        total_libraries += PQntuples(query_results[db_index]);
        PQfinish(connection);
    }

    // Allocate space for libraries + logical replication plugins
    int total_slots = count_old_cluster_logical_slots();
    os_info.libraries = pg_malloc(sizeof(LibraryInfo) * (total_libraries + total_slots));
    total_libraries = 0;

    // Phase 2: Collect library names and add logical replication plugins
    for (db_index = 0; db_index < old_cluster.dbarr.ndbs; db_index++)
    {
        PGresult *result = query_results[db_index];
        int num_rows = PQntuples(result);

        // Add C function libraries to collection
        for (int row = 0; row < num_rows; row++)
        {
            char *library_name = PQgetvalue(result, row, 0);
            os_info.libraries[total_libraries].name = pg_strdup(library_name);
            os_info.libraries[total_libraries].dbnum = db_index;
            total_libraries++;
        }
        PQclear(result);

        // Add logical replication output plugins
        LogicalSlotInfoArr *slots = &old_cluster.dbarr.dbs[db_index].slot_arr;
        for (int slot_idx = 0; slot_idx < slots->nslots; slot_idx++)
        {
            if (!slots->slots[slot_idx].invalid)
            {
                os_info.libraries[total_libraries].name = pg_strdup(slots->slots[slot_idx].plugin);
                os_info.libraries[total_libraries].dbnum = db_index;
                total_libraries++;
            }
        }
    }

    pg_free(query_results);
    os_info.num_libraries = total_libraries;
}
```