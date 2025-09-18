# get_db_infos

## Location
src/bin/pg_upgrade/info.c: 379 - 444

## Overview
The get_db_infos function scans the pg_database system catalog and populates database information for all user databases within a PostgreSQL cluster during the upgrade process.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's pg_upgrade utility that collects essential database metadata from the source cluster. It connects to the 'template1' database and executes a SQL query to retrieve database information including OID, name, encoding, collation settings, and tablespace location. The function handles version-specific differences in locale provider information across different PostgreSQL major versions (15.0+, 17.0+). The collected data is stored in the cluster's database array (dbarr) for use throughout the upgrade process.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure containing cluster connection and metadata information

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - GET_MAJOR_VERSION
  - pg_malloc0
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