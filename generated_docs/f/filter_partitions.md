# filter_partitions

## Location
[src/backend/catalog/pg_publication.c:201-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L201-L235)

## Overview
A static helper function that removes partition tables from a list of published relations when their parent/ancestor tables are already present in the same list.

## Definition

```c
static void
filter_partitions(List *table_infos)
```
## Detailed Description
This function implements partition deduplication logic for PostgreSQL publications. It iterates through a list of published relation information and removes any partition tables whose ancestor tables are already included in the publication. The function works by checking if each relation is a partition, obtaining its ancestor chain, and then verifying if any ancestor is already present in the table_infos list. If an ancestor is found, the partition is removed from the list to avoid redundancy, since publishing the parent table implicitly includes its partitions.

## Parameters / Member Variables
- `*table_infos`: A List of  structures representing tables in a publication that will be modified in-place by removing redundant partitions
## Dependencies
- Functions called/Symbols referenced:
  -  (struct type for publication relation information)
  -  (function to check if a relation is a partition)
  -  (function to get ancestor tables of a partition)
  -  (helper function to check ancestor membership)
  -  (List macro to safely delete current item during iteration)
  - , ,  (List iteration macros)
- Called from (representative examples):
  - Used internally in publication table enumeration (src/backend/catalog/pg_publication.c:1147)

## Notes and Other Information
This function modifies the input list in-place by removing elements during iteration using the foreach_delete_current macro, which is the safe way to delete list elements while iterating. The function is essential for preventing duplicate replication of partition data when both parent tables and their partitions are explicitly added to a publication. It ensures that the publication system maintains an optimal and non-redundant set of relations for replication purposes.