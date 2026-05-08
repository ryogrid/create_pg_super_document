# Catalog Inventory: Type System

Catalogs that describe the type system: pg_type, pg_cast, pg_range, pg_enum,
pg_collation, pg_conversion.

## pg_type (1247) — data types

- **Identity**: pg_type, OID 1247, header `pg_type.h`, `pg_type.dat`,
  `pg_type.c` helper.
- **Storage flags**: nailed (`BKI_BOOTSTRAP`), mapped.
- **Schema** (FormData_pg_type — abridged):
  ```c
  Oid          oid;
  NameData     typname;
  Oid          typnamespace;
  Oid          typowner;
  int16        typlen;
  bool         typbyval;
  char         typtype;          /* 'b' base, 'c' composite, 'd' domain, 'e' enum, 'p' pseudo, 'r' range, 'm' multirange */
  char         typcategory;
  bool         typispreferred;
  bool         typisdefined;
  char         typdelim;
  Oid          typrelid;          /* if composite, OID of pg_class row */
  Oid          typsubscript;
  Oid          typelem;
  Oid          typarray;
  regproc      typinput;
  regproc      typoutput;
  regproc      typreceive;
  regproc      typsend;
  regproc      typmodin;
  regproc      typmodout;
  regproc      typanalyze;
  char         typalign;
  char         typstorage;
  bool         typnotnull;
  Oid          typbasetype;
  int32        typtypmod;
  int32        typndims;
  Oid          typcollation;
  /* pg_node_tree typdefaultbin, text typdefault, aclitem[] typacl */
  ```
- **Indexes**:
  - `pg_type_oid_index` (2703): unique, (oid).
  - `pg_type_typname_nsp_index` (2704): unique, (typname, typnamespace).
- **Modification API**: `TypeCreate`, `TypeShellMake`, `TypeRename`,
  `RemoveTypeById` (pg_type.c).
- **Cache identifier**: `TYPEOID`, `TYPENAMENSP`.
- **Dependencies**: typrelid → pg_class (DEPENDENCY_INTERNAL),
  typcollation → pg_collation, typowner → pg_authid (shared),
  typnamespace → pg_namespace, plus per-function dependencies for
  typinput/typoutput/etc.
- **Bootstrap status**: yes; every built-in type ships in pg_type.dat
  (~50 types: bool, int4, text, oid, name, ...).

## pg_cast (2605) — type casts

- **Identity**: 2605, `pg_cast.h`, `pg_cast.dat`, `pg_cast.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  Oid          castsource;
  Oid          casttarget;
  Oid          castfunc;        /* InvalidOid means binary-coercible */
  char         castcontext;     /* 'e' (explicit), 'a' (assignment), 'i' (implicit) */
  char         castmethod;      /* 'f' (function), 'b' (binary), 'i' (I/O) */
  ```
- **Indexes**: `pg_cast_oid_index` (2660), `pg_cast_source_target_index` (2661, unique).
- **Modification API**: `CastCreate`, `RemoveCastById`.
- **Cache identifier**: `CASTSOURCETARGET`.
- **Dependencies**: castsource/casttarget → pg_type, castfunc → pg_proc.
- **Bootstrap status**: yes; ~200 built-in casts.

## pg_range (3541) — range type metadata

- **Identity**: 3541, `pg_range.h`, `pg_range.dat`, `pg_range.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          rngtypid;
  Oid          rngsubtype;
  Oid          rngmultitypid;
  Oid          rngcollation;
  Oid          rngsubopc;
  regproc      rngcanonical;
  regproc      rngsubdiff;
  ```
- **Indexes**: `pg_range_rngtypid_index` (3542, unique), `pg_range_rngmultitypid_index` (2228, unique).
- **Modification API**: `RangeCreate`, `RangeDelete`.
- **Cache identifier**: `RANGETYPE`, `RANGEMULTIRANGE`.
- **Bootstrap status**: yes; built-in range types int4range, int8range, numrange, tsrange, tstzrange, daterange.

## pg_enum (3501) — enum labels

- **Identity**: 3501, `pg_enum.h`, no .dat, `pg_enum.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  Oid          enumtypid;
  float4       enumsortorder;
  NameData     enumlabel;
  ```
- **Indexes**:
  - `pg_enum_oid_index` (3502, unique).
  - `pg_enum_typid_label_index` (3503, unique, (enumtypid, enumlabel)).
  - `pg_enum_typid_sortorder_index` (3534, (enumtypid, enumsortorder)).
- **Modification API**: `EnumValuesCreate`, `AddEnumLabel`,
  `RenameEnumLabel`, `RemoveEnumValueById`.
- **Cache identifier**: `ENUMOID`, `ENUMTYPOIDNAME`.
- **Dependencies**: enumtypid → pg_type (DEPENDENCY_INTERNAL — drop the type to drop labels).
- **Bootstrap status**: no.

## pg_collation (3456) — collations

- **Identity**: 3456, `pg_collation.h`, `pg_collation.dat`, `pg_collation.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  NameData     collname;
  Oid          collnamespace;
  Oid          collowner;
  char         collprovider;     /* 'b' built-in, 'c' libc, 'i' ICU */
  bool         collisdeterministic;
  int32        collencoding;
  /* text collcollate, text collctype, text colliculocale, text collicurules, text collversion */
  ```
- **Indexes**:
  - `pg_collation_oid_index` (3085, unique).
  - `pg_collation_name_enc_nsp_index` (3164, unique, (collname, collencoding, collnamespace)).
- **Modification API**: `CollationCreate`, `pg_collation_rename`.
- **Cache identifier**: `COLLOID`, `COLLNAMEENCNSP`.
- **Bootstrap status**: yes; "C", "POSIX", "default", "ucs_basic" seeded.

## pg_conversion (2607) — encoding conversions

- **Identity**: 2607, `pg_conversion.h`, `pg_conversion.dat`,
  `pg_conversion.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  NameData     conname;
  Oid          connamespace;
  Oid          conowner;
  int32        conforencoding;
  int32        contoencoding;
  Oid          conproc;
  bool         condefault;
  ```
- **Indexes**:
  - `pg_conversion_oid_index` (2669, unique).
  - `pg_conversion_name_nsp_index` (2668, unique, (conname, connamespace)).
  - `pg_conversion_default_index` (2670, (connamespace, conforencoding, contoencoding, oid)).
- **Modification API**: `ConversionCreate`, `RemoveConversionById`.
- **Cache identifier**: `CONVOID`, `CONNAMENSP`, `CONDEFAULT`.
- **Bootstrap status**: yes; many built-in conversions (UTF8 ↔ many encodings).

## Cross-references

- `component_catalog_caches.md` for syscache identifiers.
- `component_catalog_modification_apis.md` for the per-catalog helper
  functions (TypeCreate, CastCreate, etc.).
