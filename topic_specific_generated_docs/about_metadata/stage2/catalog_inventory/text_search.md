# Catalog Inventory: Text Search

## pg_ts_config (3602) — text-search configurations

- **Identity**: 3602, `pg_ts_config.h`, `pg_ts_config.dat`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      cfgname;
  Oid           cfgnamespace;
  Oid           cfgowner;
  Oid           cfgparser;
  ```
- **Indexes**:
  - `pg_ts_config_oid_index` (3712, unique).
  - `pg_ts_config_cfgname_index` (3608, unique, (cfgname, cfgnamespace)).
- **Cache identifier**: `TSCONFIGOID`, `TSCONFIGNAMENSP`.
- **Bootstrap status**: yes; ~30 built-in configurations (english,
  french, german, simple, ...).

## pg_ts_config_map (3603) — TS-config token-type → dictionary maps

- **Identity**: 3603, `pg_ts_config_map.h`, `pg_ts_config_map.dat`.
- **Schema**:
  ```c
  Oid           mapcfg;
  int32         maptokentype;
  int32         mapseqno;
  Oid           mapdict;
  ```
- **Indexes**: `pg_ts_config_map_index` (3609, unique,
  (mapcfg, maptokentype, mapseqno)).
- **Cache identifier**: `TSCONFIGMAP`.

## pg_ts_dict (3600) — text-search dictionaries

- **Identity**: 3600, `pg_ts_dict.h`, `pg_ts_dict.dat`.
- **Schema**:
  ```c
  Oid           oid;
  NameData      dictname;
  Oid           dictnamespace;
  Oid           dictowner;
  Oid           dicttemplate;
  /* text dictinitoption */
  ```
- **Indexes**:
  - `pg_ts_dict_oid_index` (3604, unique).
  - `pg_ts_dict_dictname_index` (3605, unique, (dictname, dictnamespace)).
- **Cache identifier**: `TSDICTOID`, `TSDICTNAMENSP`.

## pg_ts_parser (3601) — text-search parsers

- **Identity**: 3601, `pg_ts_parser.h`, `pg_ts_parser.dat`.
- **Schema**:
  ```c
  Oid           oid;
  NameData      prsname;
  Oid           prsnamespace;
  regproc       prsstart;
  regproc       prstoken;
  regproc       prsend;
  regproc       prsheadline;
  regproc       prslextype;
  ```
- **Indexes**:
  - `pg_ts_parser_oid_index` (3606, unique).
  - `pg_ts_parser_prsname_index` (3607, unique, (prsname, prsnamespace)).
- **Cache identifier**: `TSPARSEROID`, `TSPARSERNAMENSP`.

## pg_ts_template (3764) — text-search templates

- **Identity**: 3764, `pg_ts_template.h`, `pg_ts_template.dat`.
- **Schema**:
  ```c
  Oid           oid;
  NameData      tmplname;
  Oid           tmplnamespace;
  regproc       tmplinit;
  regproc       tmpllexize;
  ```
- **Indexes**:
  - `pg_ts_template_oid_index` (3766, unique).
  - `pg_ts_template_tmplname_index` (3765, unique).
- **Cache identifier**: `TSTEMPLATEOID`, `TSTEMPLATENAMENSP`.

## In-memory caching

`ts_cache.c` builds `TSConfigCacheEntry`, `TSDictionaryCacheEntry`,
`TSParserCacheEntry` per pg_ts_* OID. Invalidated via TSCONFIGOID, TSDICTOID,
TSPARSEROID syscache callbacks.

## Cross-references

- `component_catalog_caches.md` — ts_cache.c.
