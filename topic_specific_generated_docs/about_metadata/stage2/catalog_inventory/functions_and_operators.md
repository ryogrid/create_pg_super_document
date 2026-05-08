# Catalog Inventory: Functions, Operators, Aggregates, Languages

## pg_proc (1255) — functions and procedures

- **Identity**: pg_proc, OID 1255, header `pg_proc.h`, `pg_proc.dat`,
  `pg_proc.c` helper.
- **Storage flags**: nailed (`BKI_BOOTSTRAP`), mapped.
- **Schema** (FormData_pg_proc — abridged):
  ```c
  Oid           oid;
  NameData      proname;
  Oid           pronamespace;
  Oid           proowner;
  Oid           prolang;
  float4        procost;
  float4        prorows;
  Oid           provariadic;
  regproc       prosupport;
  char          prokind;            /* 'f' func, 'p' procedure, 'a' aggregate, 'w' window */
  bool          prosecdef;
  bool          proleakproof;
  bool          proisstrict;
  bool          proretset;
  char          provolatile;        /* 'i' immut, 's' stable, 'v' volatile */
  char          proparallel;        /* 's' safe, 'r' restricted, 'u' unsafe */
  int16         pronargs;
  int16         pronargdefaults;
  Oid           prorettype;
  oidvector     proargtypes;
  /* Oid[] proallargtypes, char[] proargmodes, text[] proargnames,
     pg_node_tree proargdefaults, Oid[] protrftypes, text prosrc, text probin,
     pg_node_tree prosqlbody, text[] proconfig, aclitem[] proacl */
  ```
- **Indexes**:
  - `pg_proc_oid_index` (2690, unique, (oid)).
  - `pg_proc_proname_args_nsp_index` (2691, unique, (proname, proargtypes, pronamespace)).
- **Modification API**: `ProcedureCreate` (pg_proc.c) — central helper used by
  CREATE FUNCTION / PROCEDURE / AGGREGATE.
- **Cache identifier**: `PROCOID`, `PROCNAMEARGSNSP`.
- **Dependencies**: prolang → pg_language, prorettype → pg_type,
  proargtypes[] → pg_type, pronamespace → pg_namespace, proowner → pg_authid.
- **Bootstrap status**: yes; pg_proc.dat ships every built-in function
  (3000+ rows).

## pg_aggregate (2600) — aggregate functions

- **Identity**: 2600, `pg_aggregate.h`, `pg_aggregate.dat`,
  `pg_aggregate.c`.
- **Storage flags**: local.
- **Schema** (key fields):
  ```c
  regproc       aggfnoid;            /* the pg_proc oid of the aggregate */
  char          aggkind;             /* 'n' normal, 'o' ordered-set, 'h' hypothetical */
  int16         aggnumdirectargs;
  regproc       aggtransfn;
  regproc       aggfinalfn;
  regproc       aggcombinefn;
  regproc       aggserialfn;
  regproc       aggdeserialfn;
  regproc       aggmtransfn;
  regproc       aggminvtransfn;
  regproc       aggmfinalfn;
  bool          aggfinalextra;
  bool          aggmfinalextra;
  char          aggfinalmodify;
  char          aggmfinalmodify;
  Oid           aggsortop;
  Oid           aggtranstype;
  int32         aggtransspace;
  Oid           aggmtranstype;
  int32         aggmtransspace;
  /* text agginitval, text aggminitval */
  ```
- **Indexes**: `pg_aggregate_fnoid_index` (2650, unique, (aggfnoid)).
- **Modification API**: `AggregateCreate` (pg_aggregate.c).
- **Cache identifier**: `AGGFNOID`.
- **Dependencies**: aggfnoid → pg_proc (DEPENDENCY_INTERNAL), trans/inv/final
  funcs → pg_proc.
- **Bootstrap status**: yes; sum, avg, count, min, max, etc. seeded.

## pg_operator (2617) — operators

- **Identity**: 2617, `pg_operator.h`, `pg_operator.dat`, `pg_operator.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      oprname;
  Oid           oprnamespace;
  Oid           oprowner;
  char          oprkind;           /* 'b' binary, 'l' left-unary */
  bool          oprcanmerge;
  bool          oprcanhash;
  Oid           oprleft;
  Oid           oprright;
  Oid           oprresult;
  Oid           oprcom;             /* commutator */
  Oid           oprnegate;
  regproc       oprcode;            /* implementing function */
  regproc       oprrest;
  regproc       oprjoin;
  ```
- **Indexes**: `pg_operator_oid_index` (2688, unique),
  `pg_operator_oprname_l_r_n_index` (2689, unique, (oprname, oprleft, oprright, oprnamespace)).
- **Modification API**: `OperatorCreate`, `OperatorShellMake`,
  `RemoveOperatorById`.
- **Cache identifier**: `OPEROID`, `OPERNAMENSP`.
- **Dependencies**: oprcode → pg_proc, oprleft/oprright/oprresult → pg_type.
- **Bootstrap status**: yes; ~700 built-in operators.

## pg_amop (2602) — operators in operator families

- **Identity**: 2602, `pg_amop.h`, `pg_amop.dat`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           amopfamily;
  Oid           amoplefttype;
  Oid           amoprighttype;
  int16         amopstrategy;
  char          amoppurpose;        /* 's' search, 'o' order */
  Oid           amopopr;             /* the operator */
  Oid           amopmethod;          /* the index AM */
  Oid           amopsortfamily;
  ```
- **Indexes**:
  - `pg_amop_oid_index` (2756, unique).
  - `pg_amop_fam_strat_index` (2754, (amopfamily, amoplefttype, amoprighttype, amopstrategy)).
  - `pg_amop_opr_fam_index` (2755, (amopopr, amoppurpose, amopfamily)).
- **Cache identifier**: `AMOPOPID`, `AMOPSTRATEGY`.

## pg_amproc (2603) — support procedures in operator families

- **Identity**: 2603, `pg_amproc.h`, `pg_amproc.dat`.
- **Schema**:
  ```c
  Oid           oid;
  Oid           amprocfamily;
  Oid           amproclefttype;
  Oid           amprocrighttype;
  int16         amprocnum;
  regproc       amproc;
  ```
- **Indexes**: `pg_amproc_oid_index` (2757, unique),
  `pg_amproc_fam_proc_index` (2655, (amprocfamily, amproclefttype, amprocrighttype, amprocnum)).
- **Cache identifier**: `AMPROCNUM`.

## pg_opclass (2616) — operator classes

- **Identity**: 2616, `pg_opclass.h`, `pg_opclass.dat`.
- **Schema**:
  ```c
  Oid           oid;
  Oid           opcmethod;
  NameData      opcname;
  Oid           opcnamespace;
  Oid           opcowner;
  Oid           opcfamily;
  Oid           opcintype;
  bool          opcdefault;
  Oid           opckeytype;
  ```
- **Indexes**: `pg_opclass_oid_index` (2687, unique),
  `pg_opclass_am_name_nsp_index` (2686, unique, (opcmethod, opcname, opcnamespace)).
- **Cache identifier**: `CLAOID`, `CLAAMNAMENSP`.

## pg_opfamily (2753) — operator families

- **Identity**: 2753, `pg_opfamily.h`, `pg_opfamily.dat`.
- **Schema**:
  ```c
  Oid           oid;
  Oid           opfmethod;
  NameData      opfname;
  Oid           opfnamespace;
  Oid           opfowner;
  ```
- **Indexes**: `pg_opfamily_oid_index` (2755, unique),
  `pg_opfamily_am_name_nsp_index` (2754, unique, (opfmethod, opfname, opfnamespace)).
- **Cache identifier**: `OPFAMILYOID`, `OPFAMILYAMNAMENSP`.

## pg_language (2612) — procedural languages

- **Identity**: 2612, `pg_language.h`, `pg_language.dat`.
- **Schema**:
  ```c
  Oid           oid;
  NameData      lanname;
  Oid           lanowner;
  bool          lanispl;
  bool          lanpltrusted;
  Oid           lanplcallfoid;
  Oid           laninline;
  Oid           lanvalidator;
  /* aclitem[] lanacl */
  ```
- **Indexes**: `pg_language_oid_index` (2681, unique), `pg_language_name_index` (2682, unique).
- **Cache identifier**: `LANGOID`, `LANGNAME`.
- **Bootstrap status**: yes; internal, c, sql, plpgsql, edb*, etc.

## Cross-references

- `component_catalog_modification_apis.md` for ProcedureCreate,
  AggregateCreate, OperatorCreate.
- `catalog_inventory/type_system.md` for the type-side of operator
  definitions.
