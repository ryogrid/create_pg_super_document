# Appendix — pg_catalog Quick Reference

[Up: index.md](index.md)  |  [Prev: appendix_data_structures.md](appendix_data_structures.md)  |  [Next: appendix_slru_quick_reference.md](appendix_slru_quick_reference.md)

One row per catalog. Use [chapter 18](18_catalog_inventory.md) for
detailed schemas and modification APIs.

Legend:
- **S** = shared (cluster-wide; lives in pg_global tablespace).
- **N** = nailed (`BKI_BOOTSTRAP`; relcache built via `formrdesc`).
- **M** = mapped (relfilenode in `pg_filenode.map`).
- **dat** = ships a `.dat` bootstrap file.
- **c** = has a `pg_<name>.c` helper file.

## Shared catalogs (11) — global/pg_filenode.map

| Name                     | OID  | Flags  | dat | c | Header                           | Primary syscache(s)                | Key indexes                                                       |
|--------------------------|------|--------|-----|---|----------------------------------|------------------------------------|-------------------------------------------------------------------|
| pg_authid                | 1260 | S,M    | yes |   | pg_authid.h                      | AUTHOID, AUTHNAME                  | AuthIdOidIndex (2676), AuthIdRolnameIndex (2677)                  |
| pg_auth_members          | 1261 | S,M    |     |   | pg_auth_members.h                | AUTHMEMRELID                       | AuthMemRoleMem (2694), AuthMemMemRole (2695), AuthMemOid (8395)    |
| pg_database              | 1262 | S,M    | yes |   | pg_database.h                    | DATABASEOID                        | DatabaseOidIndex (2672), DatabaseNameIndex (2671)                  |
| pg_db_role_setting       | 2964 | S,M    |     | y | pg_db_role_setting.h             | DATABASEROLE                        | DbRoleSettingDatidRolid (2965)                                    |
| pg_parameter_acl         | 6243 | S,M    |     | y | pg_parameter_acl.h               | PARAMETERACLOID, PARAMETERACLNAME  | ParameterAclOidIndex (6244), ParameterAclParnameIndex (6245)       |
| pg_replication_origin    | 6000 | S,M    |     |   | pg_replication_origin.h          | REPLORIGIDENT, REPLORIGNAME         | ReplicationOriginIdent (6001), ReplicationOriginName (6002)        |
| pg_shdepend              | 1214 | S,M    |     | y | pg_shdepend.h                    | (none)                              | SharedDependDepender (1232), SharedDependReference (1233)          |
| pg_shdescription         | 2396 | S,M    |     |   | pg_shdescription.h               | SHAREDDESCRIPTIONOBJ                | SharedDescriptionObjIndex (2397)                                   |
| pg_shseclabel            | 3592 | S,M    |     |   | pg_shseclabel.h                  | SHAREDSECLABELOBJECT                | SharedSecLabelObjectIndex (3593)                                   |
| pg_subscription          | 6100 | S,M    |     | y | pg_subscription.h                | SUBSCRIPTIONOID, SUBSCRIPTIONNAME  | SubscriptionObjectIndex (6114), SubscriptionNameIndex (6115)        |
| pg_tablespace            | 1213 | S,M    | yes |   | pg_tablespace.h                  | TABLESPACEOID                        | TablespaceOidIndex (2697), TablespaceNameIndex (2698)              |

## Local nailed catalogs (4) — base/<dbid>/pg_filenode.map

| Name                     | OID  | Flags  | dat | c | Header                           | Primary syscache(s)                | Key indexes                                                       |
|--------------------------|------|--------|-----|---|----------------------------------|------------------------------------|-------------------------------------------------------------------|
| pg_class                 | 1259 | N,M    | yes | y | pg_class.h                       | RELOID, RELNAMENSP                 | ClassOidIndex (2662), ClassNameNspIndex (2663), ClassTblspcRelfilenodeIndex (3455) |
| pg_attribute             | 1249 | N,M    |     |   | pg_attribute.h                   | ATTNAME, ATTNUM                    | AttributeRelidNameIndex (2658), AttributeRelidNumIndex (2659)      |
| pg_proc                  | 1255 | N,M    | yes | y | pg_proc.h                        | PROCOID, PROCNAMEARGSNSP            | ProcedureOidIndex (2690), ProcedureNameArgsNspIndex (2691)         |
| pg_type                  | 1247 | N,M    | yes | y | pg_type.h                        | TYPEOID, TYPENAMENSP               | TypeOidIndex (2703), TypeNameNspIndex (2704)                       |

## Local catalogs (48)

| Name                       | OID  | dat | c | Primary syscache(s)              | Key indexes                                                                |
|----------------------------|------|-----|---|----------------------------------|----------------------------------------------------------------------------|
| pg_aggregate               | 2600 | yes | y | AGGFNOID                         | AggregateFnoidIndex (2650)                                                |
| pg_am                      | 2601 | yes |   | AMOID, AMNAME                    | AmOidIndex (2651), AmNameIndex (2652)                                     |
| pg_amop                    | 2602 | yes |   | AMOPOPID, AMOPSTRATEGY           | AccessMethodOperatorOidIndex (2756), AccessMethodOperatorIndex (2754), AccessMethodStrategyIndex (2755) |
| pg_amproc                  | 2603 | yes |   | AMPROCNUM                        | AccessMethodProcedureOidIndex (2757), AccessMethodProcedureIndex (2655)   |
| pg_attrdef                 | 2604 |     | y | ATTRDEFOID                       | AttrDefaultOidIndex (2657), AttrDefaultIndex (2656)                       |
| pg_cast                    | 2605 | yes | y | CASTSOURCETARGET                 | CastOidIndex (2660), CastSourceTargetIndex (2661)                          |
| pg_collation               | 3456 | yes | y | COLLOID, COLLNAMEENCNSP           | CollationOidIndex (3085), CollationNameEncNspIndex (3164)                 |
| pg_constraint              | 2606 |     | y | CONSTROID, CONSTRRELOID           | ConstraintOidIndex (2667), ConstraintRelidTypidNameIndex (2664)            |
| pg_conversion              | 2607 | yes | y | CONVOID, CONNAMENSP, CONDEFAULT  | ConversionOidIndex (2669), ConversionNameNspIndex (2668)                  |
| pg_default_acl             | 826  |     |   | DEFACLROLENSPOBJ                 | DefaultAclOidIndex (827), DefaultAclRoleNspObjIndex (828)                 |
| pg_depend                  | 2608 |     | y | (none)                           | DependDependerIndex (2673), DependReferenceIndex (2674)                    |
| pg_description             | 2609 |     |   | (none)                           | DescriptionObjIndex (2675)                                                |
| pg_enum                    | 3501 |     | y | ENUMOID, ENUMTYPOIDNAME           | EnumOidIndex (3502), EnumTypIdLabelIndex (3503), EnumTypIdSortOrderIndex (3534) |
| pg_event_trigger           | 3466 |     |   | EVENTTRIGGEROID, EVENTTRIGGERNAME | EventTriggerNameIndex (3467), EventTriggerOidIndex (3468)                  |
| pg_extension               | 3079 |     |   | EXTENSIONOID, EXTENSIONNAME       | ExtensionOidIndex (3080), ExtensionNameIndex (3081)                        |
| pg_foreign_data_wrapper    | 2328 |     |   | FOREIGNDATAWRAPPEROID, FOREIGNDATAWRAPPERNAME | ForeignDataWrapperOidIndex (112), ForeignDataWrapperNameIndex (548) |
| pg_foreign_server          | 1417 |     |   | FOREIGNSERVEROID, FOREIGNSERVERNAME | ForeignServerOidIndex (113), ForeignServerNameIndex (549)                |
| pg_foreign_table           | 3118 |     |   | FOREIGNTABLEREL                  | ForeignTableRelidIndex (3119)                                              |
| pg_index                   | 2610 |     |   | INDEXRELID                       | IndexRelidIndex (2678), IndexIndrelidIndex (2679)                          |
| pg_inherits                | 2611 |     | y | (none, partcache)                | InheritsRelidSeqnoIndex (2680), InheritsParentIndex (2187)                 |
| pg_init_privs              | 3394 |     |   | (none)                           | InitPrivsObjIndex (3395)                                                   |
| pg_language                | 2612 | yes |   | LANGOID, LANGNAME                 | LanguageOidIndex (2681), LanguageNameIndex (2682)                          |
| pg_largeobject             | 2613 |     | y | (none)                           | LargeObjectLOidPNIndex (2683)                                              |
| pg_largeobject_metadata    | 2995 |     |   | LARGEOBJECTOID                    | LargeObjectMetadataOidIndex (2996)                                         |
| pg_namespace               | 2615 | yes | y | NAMESPACEOID, NAMESPACENAME       | NamespaceOidIndex (2684), NamespaceNameIndex (2685)                        |
| pg_opclass                 | 2616 | yes |   | CLAOID, CLAAMNAMENSP             | OpclassOidIndex (2687), OpclassAmNameNspIndex (2686)                       |
| pg_operator                | 2617 | yes | y | OPEROID, OPERNAMENSP              | OperatorOidIndex (2688), OperatorNameNspIndex (2689)                       |
| pg_opfamily                | 2753 | yes |   | OPFAMILYOID, OPFAMILYAMNAMENSP   | OpfamilyOidIndex (2755), OpfamilyAmNameNspIndex (2754)                     |
| pg_partitioned_table       | 3350 |     |   | PARTRELID                        | PartitionedRelidIndex (3351)                                               |
| pg_policy                  | 3256 |     |   | POLICYOID                        | PolicyOidIndex (3257), PolicyPolrelidPolnameIndex (3258)                   |
| pg_publication             | 6104 |     | y | PUBLICATIONOID, PUBLICATIONNAME   | PublicationObjectIndex (6110), PublicationNameIndex (6111)                 |
| pg_publication_namespace   | 6237 |     |   | PUBLICATIONNAMESPACE, PUBLICATIONNAMESPACEMAP | PublicationNamespaceObjectIndex (6238), PublicationNamespacePnnspidPnpubidIndex (6239) |
| pg_publication_rel         | 6106 |     |   | PUBLICATIONRELMAP, PUBLICATIONREL | PublicationRelObjectIndex (6112), PublicationRelPrrelidPrpubidIndex (6113) |
| pg_range                   | 3541 | yes | y | RANGETYPE, RANGEMULTIRANGE        | RangeTypidIndex (3542), RangeMultiRangeTypidIndex (2228)                   |
| pg_rewrite                 | 2618 |     |   | RULERELNAME                      | RewriteOidIndex (2692), RewriteRelRulenameIndex (2693)                     |
| pg_seclabel                | 3596 |     |   | (none)                           | SecLabelObjectIndex (3597)                                                |
| pg_sequence                | 2224 |     |   | SEQRELID                         | SequenceRelidIndex (5002)                                                 |
| pg_statistic               | 2619 |     |   | STATRELATTINH                    | StatisticRelidAttnumInhIndex (2696)                                       |
| pg_statistic_ext           | 3381 |     |   | STATEXTOID, STATEXTNAMENSP        | StatisticExtOidIndex (3380), StatisticExtNameIndex (3997), StatisticExtRelidIndex (3379) |
| pg_statistic_ext_data      | 3429 |     |   | STATEXTDATASTXOID                | StatisticExtDataStxoidInhIndex (3430)                                     |
| pg_subscription_rel        | 6102 |     |   | SUBSCRIPTIONRELMAP                | SubscriptionRelSrrelidSrsubidIndex (6117)                                  |
| pg_transform               | 3576 |     |   | TRFOID, TRFTYPELANG              | TransformOidIndex (3574), TransformTypeLangIndex (3575)                   |
| pg_trigger                 | 2620 |     |   | TRGOID, TRGRELID                 | TriggerOidIndex (2702), TriggerRelidNameIndex (2701)                       |
| pg_ts_config               | 3602 | yes |   | TSCONFIGOID, TSCONFIGNAMENSP      | TSConfigOidIndex (3712), TSConfigNameNspIndex (3608)                       |
| pg_ts_config_map           | 3603 | yes |   | TSCONFIGMAP                       | TSConfigMapIndex (3609)                                                   |
| pg_ts_dict                 | 3600 | yes |   | TSDICTOID, TSDICTNAMENSP          | TSDictionaryOidIndex (3604), TSDictionaryNameNspIndex (3605)               |
| pg_ts_parser               | 3601 | yes |   | TSPARSEROID, TSPARSERNAMENSP      | TSParserOidIndex (3606), TSParserNameNspIndex (3607)                       |
| pg_ts_template             | 3764 | yes |   | TSTEMPLATEOID, TSTEMPLATENAMENSP  | TSTemplateOidIndex (3766), TSTemplateNameNspIndex (3765)                   |
| pg_user_mapping            | 1418 |     |   | USERMAPPINGOID, USERMAPPINGUSERSERVER | UserMappingOidIndex (174), UserMappingUserServerIndex (175)            |

**Total**: 11 shared + 4 nailed + 48 local = **63 catalogs**.

## See also

- Detailed schemas, dependencies, and SQL examples: chapter
  [18 Catalog Inventory](18_catalog_inventory.md).
- Nailed/shared/mapped semantics: chapter
  [03 Catalog Data Model](03_catalog_data_model_and_bootstrap.md).
- Modification entry points: chapter
  [04 Catalog Modification APIs](04_catalog_modification_apis.md).
- Cache identifiers: chapter [05 Catalog Caches](05_catalog_caches.md).

---

[Up: index.md](index.md)  |  [Prev: appendix_data_structures.md](appendix_data_structures.md)  |  [Next: appendix_slru_quick_reference.md](appendix_slru_quick_reference.md)
