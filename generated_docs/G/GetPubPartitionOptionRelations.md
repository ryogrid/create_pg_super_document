# GetPubPartitionOptionRelations

## Location
[src/backend/catalog/pg_publication.c:267-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L267-L310)

## Overview
A function that determines which relations to include in a publication based on the specified partition option for a given table.

## Definition

```c
List *
GetPubPartitionOptionRelations(List *result, PublicationPartOpt pub_partopt,
							   Oid relid)
```
## Detailed Description
This function implements the logic for handling different partition publication strategies in PostgreSQL logical replication. Based on the PublicationPartOpt setting, it determines which relations should be included when a partitioned table is added to a publication. For partitioned tables, it can include all partitions (PUBLICATION_PART_ALL), only leaf partitions (PUBLICATION_PART_LEAF), or just the root table (PUBLICATION_PART_ROOT). For non-partitioned tables, it simply adds the table itself to the result list. The function uses find_all_inheritors to discover all partitions and filters them according to the specified option.

## Parameters / Member Variables
- `*result`: An existing List of relation OIDs that will be extended with new relations based on the partition option
- `pub_partopt`: The publication partition option enum value specifying how partitions should be handled (ALL, LEAF, or ROOT)
- `relid`: The OID of the relation to process for partition inclusion
## Dependencies
- Functions called/Symbols referenced:
  -  (enum type defining partition publication options)
  -  (function to get relation kind)
  -  (function to find all partition descendants)
  -  (function to concatenate lists)
  -  (function to append OID to list)
  - , ,  (enum constants)
  -  (constant for partitioned table relation kind)
  - ,  (List iteration macros)
- Called from (representative examples):
  -  (src/backend/catalog/pg_publication.c:471)
  -  (src/backend/catalog/pg_publication.c:741)
  -  (src/backend/catalog/pg_publication.c:965)
  -  (src/backend/commands/publicationcmds.c:1035)
  -  (src/backend/commands/publicationcmds.c:1466)

## Notes and Other Information
This function is central to PostgreSQL's flexible partition publication system, allowing users to control which parts of a partitioned table hierarchy are replicated. The PUBLICATION_PART_ALL option includes all partitions for comprehensive replication, PUBLICATION_PART_LEAF includes only leaf partitions to avoid redundancy, and PUBLICATION_PART_ROOT includes only the parent table. The function modifies and returns the input result list, making it suitable for use in building comprehensive relation lists for publication operations.

## Simplified Source

```c
List *GetPubPartitionOptionRelations(List *result, PublicationPartOpt pub_partopt,
                                     Oid relid) {
    if (get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE &&
        pub_partopt != PUBLICATION_PART_ROOT) {

        // Find all partitions of this partitioned table
        List *all_parts = find_all_inheritors(relid, NoLock, NULL);

        if (pub_partopt == PUBLICATION_PART_ALL) {
            // Include all partitions
            result = list_concat(result, all_parts);
        } else if (pub_partopt == PUBLICATION_PART_LEAF) {
            // Include only leaf partitions (non-partitioned)
            ListCell *lc;
            foreach(lc, all_parts) {
                Oid partOid = lfirst_oid(lc);
                if (get_rel_relkind(partOid) != RELKIND_PARTITIONED_TABLE)
                    result = lappend_oid(result, partOid);
            }
        } else {
            Assert(false);  // Invalid partition option
        }
    } else {
        // For non-partitioned tables or PUBLICATION_PART_ROOT option
        result = lappend_oid(result, relid);
    }

    return result;
}
```