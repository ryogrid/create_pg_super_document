# Catalog Inventory: Replication and Publication

## pg_publication (6104) — logical-replication publications

- **Identity**: 6104, `pg_publication.h`, no .dat, `pg_publication.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      pubname;
  Oid           pubowner;
  bool          puballtables;
  bool          puballsequences;
  bool          pubinsert;
  bool          pubupdate;
  bool          pubdelete;
  bool          pubtruncate;
  bool          pubviaroot;
  ```
- **Indexes**:
  - `pg_publication_oid_index` (6110, unique).
  - `pg_publication_pubname_index` (6111, unique).
- **Modification API**: `CreatePublication`, `AlterPublication`,
  `RemovePublicationById`.
- **Cache identifier**: `PUBLICATIONOID`, `PUBLICATIONNAME`.

## pg_publication_rel (6106) — publications → tables mapping

- **Identity**: 6106, `pg_publication_rel.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           prpubid;          /* publication */
  Oid           prrelid;          /* relation */
  /* pg_node_tree prqual, int2vector prattrs */
  ```
- **Indexes**:
  - `pg_publication_rel_oid_index` (6112, unique).
  - `pg_publication_rel_prrelid_prpubid_index` (6113, unique).
- **Modification API**: `publication_add_relation`,
  `RemovePublicationRelById`.
- **Cache identifier**: `PUBLICATIONRELMAP`, `PUBLICATIONREL`.

## pg_publication_namespace (6237) — publications → schemas

- **Identity**: 6237, `pg_publication_namespace.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           pnpubid;
  Oid           pnnspid;
  ```
- **Indexes**:
  - `pg_publication_namespace_object_index` (6238, unique).
  - `pg_publication_namespace_pnnspid_pnpubid_index` (6239, unique).
- **Modification API**: `publication_add_schema`,
  `RemovePublicationSchemaById`.
- **Cache identifier**: `PUBLICATIONNAMESPACE`, `PUBLICATIONNAMESPACEMAP`.

## pg_subscription (6100) — logical-replication subscriptions

- **Identity**: 6100, shared, mapped, `pg_subscription.c`.
- **Schema**:
  ```c
  Oid           oid;
  Oid           subdbid;
  int32         subskiplsn;
  NameData      subname;
  Oid           subowner;
  bool          subenabled;
  bool          subbinary;
  bool          substream;
  bool          subtwophasestate;
  bool          subdisableonerr;
  bool          subpasswordrequired;
  bool          subrunasowner;
  bool          subfailover;
  /* text subconninfo, NameData subslotname, text subsynccommit, text[] subpublications,
     text suborigin */
  ```
- **Indexes**:
  - `pg_subscription_oid_index` (6114, unique).
  - `pg_subscription_subname_index` (6115, unique, (subdbid, subname)).
- **Modification API**: `CreateSubscription`, `AlterSubscription`,
  `DropSubscription` (`commands/subscriptioncmds.c`).
- **Cache identifier**: `SUBSCRIPTIONOID`, `SUBSCRIPTIONNAME`.

## pg_subscription_rel (6102) — per-subscription per-relation state

- **Identity**: 6102, `pg_subscription_rel.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           srsubid;
  Oid           srrelid;
  char          srsubstate;        /* 'i' init, 'd' data sync, 's' synced, 'r' ready */
  XLogRecPtr    srsublsn;
  ```
- **Indexes**: `pg_subscription_rel_srrelid_srsubid_index` (6117, unique,
  (srrelid, srsubid)).
- **Modification API**: `AddSubscriptionRelState`,
  `UpdateSubscriptionRelState`.
- **Cache identifier**: `SUBSCRIPTIONRELMAP`.

## pg_replication_origin (6000) — replication origins

- **Identity**: 6000, shared, mapped.
- **Schema**:
  ```c
  Oid           roident;
  /* text roname */
  ```
- **Indexes**:
  - `pg_replication_origin_roident_index` (6001, unique).
  - `pg_replication_origin_roname_index` (6002, unique).
- **Modification API**: `replorigin_create`, `replorigin_drop_by_name`
  (`replication/logical/origin.c`).
- **Cache identifier**: `REPLORIGIDENT`, `REPLORIGNAME`.

## Cross-references

- `component_persistence_and_wal_records.md` — XLOG_XACT_COMMIT carries
  RepOriginId (used by pg_subscription's `subskiplsn`).
- `component_commit_ts.md` — RepOriginId stored alongside commit timestamps.
