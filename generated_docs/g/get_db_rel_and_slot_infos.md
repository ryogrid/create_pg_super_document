# get_db_rel_and_slot_infos

## Location
[src/bin/pg_upgrade/info.c:279-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L279-L313)

## Overview
High-level routine that retrieves comprehensive database, relation, and logical replication slot information for a PostgreSQL cluster during upgrade operations.

## Definition

```c
void
get_db_rel_and_slot_infos(ClusterInfo *cluster, bool live_check)
```
## Detailed Description
This function serves as the main entry point for gathering all necessary database metadata from a PostgreSQL cluster during pg_upgrade operations. It orchestrates the collection of template0 information, database listings, relation information for each database, and logical replication slot information for old clusters.

The function operates in a systematic manner: first obtaining basic cluster information, then iterating through all databases to gather detailed relation metadata. For old clusters, it additionally collects logical replication slot information which is crucial for maintaining replication continuity during upgrades.

The function also handles memory management by freeing previously allocated database information and provides verbose logging to help administrators track the information gathering process.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure representing the PostgreSQL cluster to analyze
- : Boolean flag used for live checking functionality (only applicable when target is the old cluster)

## Dependencies
- Functions called/Symbols referenced:
  - [free_db_and_rel_infos](../f/free_db_and_rel_infos.md)
  - [get_template0_info](get_template0_info.md)
  - [get_db_infos](get_db_infos.md)
  - [get_rel_infos](get_rel_infos.md)
  - [get_old_cluster_logical_slot_infos](get_old_cluster_logical_slot_infos.md)
  - [pg_log](../p/pg_log.md)
  - [print_db_infos](../p/print_db_infos.md)
- Data structures used:
  - ClusterInfo
  - [DbInfo](../D/DbInfo.md)
- Global variables accessed:
  - old_cluster
  - log_opts
- Called from (representative examples):
  - [check_and_dump_old_cluster](../c/check_and_dump_old_cluster.md)
  - [check_new_cluster](../c/check_new_cluster.md)
  - [create_new_objects](../c/create_new_objects.md)

## Notes and Other Information
- Performs memory cleanup by freeing existing database arrays before repopulating them
- Specifically handles logical replication slots only for old clusters, not new ones
- Provides verbose logging that differentiates between source (old) and target (new) databases
- Central orchestration function that coordinates multiple information-gathering subsystems
- Part of pg_upgrade's cluster analysis and preparation infrastructure
- Essential for building the complete picture of cluster contents before performing upgrade operations