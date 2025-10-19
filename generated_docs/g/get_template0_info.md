# get_template0_info

## Location
[src/bin/pg_upgrade/info.c:314-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L314-L378)

## Overview
Retrieves locale and encoding information from the template0 database, which serves as the base template that will be copied from the old cluster to the new cluster during PostgreSQL upgrades.

## Definition

```c
static void
get_template0_info(ClusterInfo *cluster)
```
## Detailed Description
This static function connects to the template1 database to query information about template0, which is crucial for maintaining locale and encoding consistency during PostgreSQL upgrades. The function handles version-specific differences in how locale information is stored and accessed across different PostgreSQL major versions.

The function adapts its query based on the PostgreSQL version, accommodating changes in locale provider functionality and column names. For PostgreSQL 17.0+, it uses the standard datlocale field; for 15.0-16.x, it uses daticulocale aliased as datlocale; and for older versions, it provides compatibility by using hardcoded values and NULL for unsupported fields.

The retrieved information is stored in the cluster's template0 field for later use during the upgrade process, ensuring that new databases created in the target cluster maintain the same locale characteristics as the source cluster.

## Parameters / Member Variables
- `*cluster`: Pointer to ClusterInfo structure representing the PostgreSQL cluster being analyzed
## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - GET_MAJOR_VERSION
  - [pg_malloc](../p/pg_malloc.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - [pg_fatal](../p/pg_fatal.md)
  - atoi
- Data structures used:
  - ClusterInfo
  - DbLocaleInfo
  - [PGconn](../P/PGconn.md)
  - [PGresult](../P/PGresult.md)
- Called from (representative examples):
  - [get_db_rel_and_slot_infos](get_db_rel_and_slot_infos.md)

## Notes and Other Information
- Static function - only accessible within the same source file (info.c)
- Connects to template1 database to query information about template0 database
- Handles PostgreSQL version compatibility for locale provider changes introduced in version 15.0
- Uses different query strategies based on major version: 17.0+, 15.0-16.x, and pre-15.0
- Stores encoding, collation provider, collate, ctype, and locale information
- Critical for maintaining database locale consistency during pg_upgrade operations
- Memory allocated for DbLocaleInfo must be managed by the calling code
- Part of pg_upgrade's cluster information gathering infrastructure

## Simplified Source

```c
static void
get_template0_info(ClusterInfo *cluster)
{
    PGconn *connection = connectToServer(cluster, "template1");
    DbLocaleInfo *locale_info;
    PGresult *query_result;

    // Build version-specific query for template0 locale information
    if (GET_MAJOR_VERSION(cluster->major_version) >= 1700)
    {
        // PostgreSQL 17.0+ uses standard datlocale field
        query_result = executeQueryOrDie(connection,
            "SELECT encoding, datlocprovider, datcollate, datctype, datlocale "
            "FROM pg_catalog.pg_database WHERE datname='template0'");
    }
    else if (GET_MAJOR_VERSION(cluster->major_version) >= 1500)
    {
        // PostgreSQL 15.0-16.x uses daticulocale aliased as datlocale
        query_result = executeQueryOrDie(connection,
            "SELECT encoding, datlocprovider, datcollate, datctype, daticulocale AS datlocale "
            "FROM pg_catalog.pg_database WHERE datname='template0'");
    }
    else
    {
        // Pre-15.0 versions: provide compatibility defaults
        query_result = executeQueryOrDie(connection,
            "SELECT encoding, 'c' AS datlocprovider, datcollate, datctype, NULL AS datlocale "
            "FROM pg_catalog.pg_database WHERE datname='template0'");
    }

    // Verify template0 exists
    if (PQntuples(query_result) != 1)
        pg_fatal("template0 not found");

    // Allocate and populate locale information structure
    locale_info = pg_malloc(sizeof(DbLocaleInfo));

    // Extract field indices for result parsing
    int encoding_idx = PQfnumber(query_result, "encoding");
    int provider_idx = PQfnumber(query_result, "datlocprovider");
    int collate_idx = PQfnumber(query_result, "datcollate");
    int ctype_idx = PQfnumber(query_result, "datctype");
    int locale_idx = PQfnumber(query_result, "datlocale");

    // Parse and store locale information
    locale_info->db_encoding = atoi(PQgetvalue(query_result, 0, encoding_idx));
    locale_info->db_collprovider = PQgetvalue(query_result, 0, provider_idx)[0];
    locale_info->db_collate = pg_strdup(PQgetvalue(query_result, 0, collate_idx));
    locale_info->db_ctype = pg_strdup(PQgetvalue(query_result, 0, ctype_idx));

    // Handle potentially NULL locale field
    if (PQgetisnull(query_result, 0, locale_idx))
        locale_info->db_locale = NULL;
    else
        locale_info->db_locale = pg_strdup(PQgetvalue(query_result, 0, locale_idx));

    // Store in cluster information
    cluster->template0 = locale_info;

    PQclear(query_result);
    PQfinish(connection);
}
```