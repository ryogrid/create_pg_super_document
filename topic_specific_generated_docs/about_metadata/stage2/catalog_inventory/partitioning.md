# Catalog Inventory: Partitioning

## pg_partitioned_table (3350) — per-partitioned-table metadata

- **Identity**: 3350, header `pg_partitioned_table.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           partrelid;          /* OID of the partitioned table */
  char          partstrat;          /* 'r' RANGE, 'l' LIST, 'h' HASH */
  int16         partnatts;
  Oid           partdefid;          /* OID of default partition */
  int2vector    partattrs;          /* column attnums; 0 = expression */
  /* Oid[] partclass, Oid[] partcollation, pg_node_tree partexprs */
  ```
- **Indexes**: `pg_partitioned_table_partrelid_index` (3351, unique, (partrelid)).
- **Modification API**:
  - `StorePartitionKey` (`src/backend/catalog/partition.c`).
  - `RemovePartitionKeyByRelId`.
  - `pg_partitioned_table_aclmask` (rare; usually inherits from pg_class).
- **Cache identifier**: `PARTRELID`.
- **Dependencies**: partrelid → pg_class (DEPENDENCY_INTERNAL — drop the
  table to drop this row), partdefid → pg_class.

## pg_inherits (2611)

Already documented in `constraints_and_dependencies.md`. For partitioning,
`pg_inherits` is the source-of-truth for the parent-child relationship:
each partition is a `pg_inherits` row with `inhparent` = partitioned table.

The partition is indicated by `pg_class.relispartition = true`. The
`relpartbound` text node in pg_class describes the bound spec
(`FOR VALUES FROM (...) TO (...)`, etc.).

## pg_class partition flags

`pg_class` carries two partition-related fields:

| Field            | Meaning                                                   |
|------------------|-----------------------------------------------------------|
| `relispartition` | true iff this row is a partition of some parent           |
| `relkind = 'p'`  | this is a partitioned table (has children)                |
| `relpartbound`   | pg_node_tree representing the partition bound expression  |
| `relrewrite`     | for ATTACH PARTITION CONCURRENTLY: temp ID during rewrite |

## In-memory representation

`partcache.c::RelationGetPartitionDesc(rel)` builds a `PartitionDesc` with:

```c
typedef struct PartitionDescData
{
    int                nparts;         /* # partitions */
    bool               detached_exist;  /* any DETACH PENDING child? */
    Oid               *oids;            /* OIDs sorted by bound */
    bool              *is_leaf;
    PartitionBoundInfo boundinfo;        /* the bound-array structure */
} PartitionDescData;
```

Built from a snapshot of pg_inherits + pg_class.relpartbound rows. Cached
on the parent's RelationData via `rd_partdesc`. Invalidated on RELOID
syscache callback.

`partition.c::RelationGetPartitionKey(rel)` builds a `PartitionKey` with
column attnums, collations, opclass, partkey expressions. Cached on
`rd_partkey`.

## Partition-routing

`execPartition.c` uses the cached PartitionDesc + PartitionKey to route
INSERT row tuples to the correct child. Repeated invocations skip the
rebuild via the relcache cache.

## Partition pruning

`partprune.c` reads the same cached structures and produces a list of
"surviving" partition OIDs after evaluating the WHERE clause's
constraints against the partition bounds.

## Cross-references

- `component_catalog_caches.md` — partcache.c.
- `component_catalog_modification_apis.md` — partition-related DDL paths.
- `catalog_inventory/core_relations.md` — pg_class, pg_inherits.
