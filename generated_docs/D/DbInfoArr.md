# DbInfoArr

## Location
src/bin/pg_upgrade/pg_upgrade.h: 218 - 248

## Overview
DbInfoArr is a container structure that holds an array of database information entries, providing a comprehensive collection of all databases in a PostgreSQL cluster during the pg_upgrade process.

## Definition


## Detailed Description
DbInfoArr serves as the top-level container for managing database information during PostgreSQL cluster upgrades. It provides a structured way to handle multiple databases within a cluster, enabling pg_upgrade to process all databases systematically. This structure is fundamental to the upgrade architecture as it represents the complete database inventory of both source and target clusters.

## Parameters / Member Variables
- : Pointer to an array of DbInfo structures, each containing comprehensive information about an individual database
- : Integer count representing the total number of databases in the array

## Dependencies
- Functions called/Symbols referenced:
  - [DbInfo](DbInfo.md)
  - ident (indirectly through related structures)
- Called from (representative examples):
  - [free_db_and_rel_infos](../f/free_db_and_rel_infos.md)
  - [print_db_infos](../p/print_db_infos.md)
  - [parallel_transfer_all_new_dbs](../p/parallel_transfer_all_new_dbs.md)
  - [transfer_all_new_tablespaces](../t/transfer_all_new_tablespaces.md)
  - [transfer_all_new_dbs](../t/transfer_all_new_dbs.md)

## Notes and Other Information
- This structure is central to cluster-wide operations during pg_upgrade, enabling batch processing of all databases
- Used extensively in parallel processing scenarios where multiple databases can be processed simultaneously
- Memory management for the dbs array should follow PostgreSQL's standard allocation patterns
- Critical for maintaining database inventory consistency between old and new clusters
- The structure facilitates both sequential and parallel database processing strategies
- Essential for comprehensive cluster migration including all user and system databases (excluding template databases typically)