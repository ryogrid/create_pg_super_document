# table_relation_copy_for_cluster

## Location
[src/include/access/tableam.h:1679-1707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1679-L1707)

## Overview
A table access method wrapper function that copies data from an old table to a new table as part of CLUSTER or VACUUM FULL operations, with support for sorting and transaction visibility management.

## Definition

```c
static inline void
table_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
								Relation OldIndex,
								bool use_sort,
								TransactionId OldestXmin,
								TransactionId *xid_cutoff,
								MultiXactId *multi_cutoff,
								double *num_tuples,
								double *tups_vacuumed,
								double *tups_recently_dead)
```
## Detailed Description
This function is the core data copying mechanism for CLUSTER and VACUUM FULL operations. It handles the complex process of copying data from an old table to a new table while managing transaction visibility, sorting requirements, and statistical collection.

The function provides flexible sorting behavior: it can sort data according to an index, copy data in index order without additional sorting, or copy data without any ordering. This flexibility allows optimization for different clustering scenarios. The function also manages transaction visibility by establishing appropriate cutoff points for frozen transactions and multixacts.

During the copy process, the function collects important statistics about tuple processing, including the number of tuples vacuumed and recently dead tuples, which are essential for vacuum operation reporting and future vacuum scheduling decisions.

## Parameters / Member Variables
- `OldTable`: The source relation being copied from
- `NewTable`: The destination relation being copied to
- `OldIndex`: Index to use for ordering (can be InvalidOid if no index-based ordering)
- `use_sort`: If true, sort table contents appropriately for OldIndex; if false, copy in index order or no specific order
- `OldestXmin`: Transaction visibility horizon computed by vacuum_get_cutoffs()
- `*xid_cutoff`: Output parameter for the new relfrozenxid value (may be invalid)
- `*multi_cutoff`: Output parameter for the new relminmxid value (may be invalid)
- `*num_tuples`: Output parameter for total number of tuples processed
- `*tups_vacuumed`: Output parameter for count of tuples vacuumed (for logging)
- `*tups_recently_dead`: Output parameter for count of recently dead tuples (for logging)
## Dependencies
- Functions called/Symbols referenced:
  - OldTable->rd_tableam->relation_copy_for_cluster (table access method implementation)
  - TransactionId (transaction management type)
  - MultiXactId (multixact management type)
- Called from (representative examples):
  - [copy_table_data](../c/copy_table_data.md) (during cluster operations)

## Notes and Other Information
- This is the primary mechanism for CLUSTER and VACUUM FULL data reorganization
- The function handles both sorted and unsorted copy operations based on parameters
- Statistics collection is crucial for vacuum operation reporting and future planning
- Transaction visibility management ensures MVCC consistency during the copy process
- The function delegates to table access method implementations for storage-specific optimizations
- Performance is critical as this function processes all table data during clustering operations

## Simplified Source

```c
static inline void
table_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
                                Relation OldIndex,
                                bool use_sort,
                                TransactionId OldestXmin,
                                TransactionId *xid_cutoff,
                                MultiXactId *multi_cutoff,
                                double *num_tuples,
                                double *tups_vacuumed,
                                double *tups_recently_dead)
{
    // Delegate to the table access method's copy implementation
    OldTable->rd_tableam->relation_copy_for_cluster(OldTable, NewTable, OldIndex,
                                                    use_sort, OldestXmin,
                                                    xid_cutoff, multi_cutoff,
                                                    num_tuples, tups_vacuumed,
                                                    tups_recently_dead);
}
```