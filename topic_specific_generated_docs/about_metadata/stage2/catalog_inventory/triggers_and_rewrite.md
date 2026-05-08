# Catalog Inventory: Triggers and Rewrite Rules

## pg_trigger (2620) — triggers

- **Identity**: 2620, `pg_trigger.h`, no .dat.
- **Storage flags**: local.
- **Schema** (key fields):
  ```c
  Oid           oid;
  Oid           tgrelid;
  Oid           tgparentid;
  NameData      tgname;
  Oid           tgfoid;             /* trigger function */
  int16         tgtype;              /* bitmask: BEFORE/AFTER/INSTEAD; INSERT/UPDATE/DELETE/TRUNCATE; ROW/STATEMENT */
  char          tgenabled;           /* 'O' origin, 'A' always, 'R' replica, 'D' disabled */
  bool          tgisinternal;
  bool          tgisclone;
  Oid           tgconstrrelid;
  Oid           tgconstrindid;
  Oid           tgconstraint;
  bool          tgdeferrable;
  bool          tginitdeferred;
  int16         tgnargs;
  /* int2vector tgattr, bytea tgargs, pg_node_tree tgqual, NameData tgoldtable, NameData tgnewtable */
  ```
- **Indexes**:
  - `pg_trigger_oid_index` (2702, unique).
  - `pg_trigger_tgrelid_tgname_index` (2701, unique, (tgrelid, tgname)).
  - `pg_trigger_tgconstraint_index` (2699, (tgconstraint)).
- **Modification API**: `CreateTrigger` (`trigger.c`),
  `RemoveTriggerById`, `EnableDisableTrigger`.
- **Cache identifier**: `TRGOID`, `TRGRELID`.
- **Dependencies**: tgrelid → pg_class (DEPENDENCY_AUTO),
  tgfoid → pg_proc, tgconstraint → pg_constraint.

## pg_event_trigger (3466) — event-trigger registrations

- **Identity**: 3466, `pg_event_trigger.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      evtname;
  NameData      evtevent;
  Oid           evtowner;
  Oid           evtfoid;
  char          evtenabled;
  /* text[] evttags */
  ```
- **Indexes**:
  - `pg_event_trigger_evtname_index` (3467, unique).
  - `pg_event_trigger_oid_index` (3468, unique).
- **Modification API**: `CreateEventTrigger`,
  `RemoveEventTriggerById`.
- **Cache identifier**: `EVENTTRIGGEROID`, `EVENTTRIGGERNAME`.

## pg_rewrite (2618) — rewrite rules (views, ON SELECT/INSERT rules)

- **Identity**: 2618, `pg_rewrite.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      rulename;
  Oid           ev_class;          /* relation */
  char          ev_type;           /* '1' SELECT, '2' UPDATE, '3' INSERT, '4' DELETE */
  char          ev_enabled;
  bool          is_instead;
  /* pg_node_tree ev_qual, pg_node_tree ev_action */
  ```
- **Indexes**:
  - `pg_rewrite_oid_index` (2692, unique).
  - `pg_rewrite_rel_rulename_index` (2693, unique, (ev_class, rulename)).
- **Modification API**: `DefineRule`, `RewriteQuery`,
  `RemoveRewriteRuleById`.
- **Cache identifier**: `RULERELNAME`.
- **Dependencies**: ev_class → pg_class (DEPENDENCY_AUTO).

## In-memory representations

- Triggers: `RelationData::trigdesc` (TriggerDesc) — built from pg_trigger.
- Event triggers: cached via `evtcache.c::EventCacheLookup`.
- Rewrite rules: `RelationData::rd_rules` (RuleLock).

## Cross-references

- `component_catalog_caches.md` — evtcache, RelationData::trigdesc.
- `catalog_inventory/core_relations.md` — pg_class.relhastriggers,
  relhasrules.
