# Catalog Inventory: Access Control

Catalogs that govern role membership, ACLs, default privileges, security
labels, RLS policies, and parameter ACLs.

## pg_authid (1260)

See `core_relations.md`. Stores roles (login users + groups). Shared, mapped,
.dat.

## pg_auth_members (1261)

See `core_relations.md`. Role-to-role grant table. Shared, mapped.

## pg_database (1262)

See `core_relations.md`. Per-database privileges live in `datacl`. Shared,
mapped.

## pg_tablespace (1213)

See `core_relations.md`. Tablespace privileges live in `spcacl`.

## pg_default_acl (826) — ALTER DEFAULT PRIVILEGES

- **Identity**: 826, `pg_default_acl.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           defaclrole;
  Oid           defaclnamespace;
  char          defaclobjtype;     /* 'r' rel, 'S' seq, 'f' func, 'T' type, 'n' schema */
  /* aclitem[] defaclacl */
  ```
- **Indexes**:
  - `pg_default_acl_oid_index` (827, unique).
  - `pg_default_acl_role_nsp_obj_index` (828, unique,
    (defaclrole, defaclnamespace, defaclobjtype)).
- **Modification API**: `ExecAlterDefaultPrivilegesStmt`,
  `RemoveDefaultACLById`.
- **Cache identifier**: `DEFACLROLENSPOBJ`.

## pg_init_privs (3394) — original privileges (for pg_dump)

- **Identity**: 3394, `pg_init_privs.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           classoid;
  Oid           objoid;
  int32         objsubid;
  char          privtype;
  /* aclitem[] initprivs */
  ```
  `privtype`: `'i'` (initdb-time) or `'e'` (extension).

- **Indexes**: `pg_init_privs_o_c_o_index` (3395, unique,
  (objoid, classoid, objsubid)).
- **Modification API**: `recordExtensionInitPriv` (extension.c),
  `recordExtObjInitPriv`.

## pg_policy (3256) — RLS policies

- **Identity**: 3256, `pg_policy.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      polname;
  Oid           polrelid;
  char          polcmd;          /* 'r' SELECT, 'a' INSERT, 'w' UPDATE, 'd' DELETE, '*' ALL */
  bool          polpermissive;
  /* Oid[] polroles, pg_node_tree polqual, pg_node_tree polwithcheck */
  ```
- **Indexes**:
  - `pg_policy_oid_index` (3257, unique).
  - `pg_policy_polrelid_polname_index` (3258, unique,
    (polrelid, polname)).
- **Modification API**: `CreatePolicy`, `RenamePolicy`, `RemovePolicyById`
  (`commands/policy.c`).
- **Cache identifier**: `POLICYOID`.
- **Dependencies**: polrelid → pg_class (DEPENDENCY_AUTO),
  polroles[] → pg_authid via pg_shdepend.

## pg_seclabel (3596) — security labels

- **Identity**: 3596, `pg_seclabel.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           objoid;
  Oid           classoid;
  int32         objsubid;
  text          provider;        /* "selinux", etc. */
  text          label;
  ```
- **Indexes**: `pg_seclabel_object_index` (3597, unique,
  (objoid, classoid, objsubid, provider)).
- **Modification API**: `SetSecurityLabel`, `DeleteSecurityLabel`.

## pg_shseclabel (3592) — security labels for shared objects

- **Identity**: 3592, shared, mapped.
- **Schema**:
  ```c
  Oid           objoid;
  Oid           classoid;
  text          provider;
  text          label;
  ```
- **Indexes**: `pg_shseclabel_object_index` (3593, unique).
- **Modification API**: same as pg_seclabel; routed by class.

## pg_parameter_acl (6243) — ACLs on configuration parameters

- **Identity**: 6243, shared, mapped, `pg_parameter_acl.c`.
- **Schema**:
  ```c
  Oid           oid;
  text          parname;
  /* aclitem[] paracl */
  ```
- **Indexes**:
  - `pg_parameter_acl_oid_index` (6244, unique).
  - `pg_parameter_acl_parname_index` (6245, unique).
- **Modification API**: `ParameterAclCreate`, `ParameterAclLookup`.
- **Cache identifier**: `PARAMETERACLOID`, `PARAMETERACLNAME`.

## Cross-references

- `component_catalog_modification_apis.md` — `aclchk.c::ExecGrantStmt_oids`.
- `component_catalog_caches.md` — RLS policy invalidation via partcache.
