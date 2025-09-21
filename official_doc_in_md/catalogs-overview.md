51.1. Overview  
---  
[Prev](catalogs.md "Chapter 51. System Catalogs") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-aggregate.md "51.2. pg_aggregate")  
  
* * *

## 51.1. Overview #

[Table 51.1](catalogs-overview.md#CATALOG-TABLE "Table 51.1. System Catalogs") lists the system catalogs. More detailed documentation of each catalog follows below. 

Most system catalogs are copied from the template database during database creation and are thereafter database-specific. A few catalogs are physically shared across all databases in a cluster; these are noted in the descriptions of the individual catalogs. 

**Table 51.1. System Catalogs**

Catalog Name| Purpose  
---|---  
[`pg_aggregate`](catalog-pg-aggregate.md "51.2. pg_aggregate")| aggregate functions  
[`pg_am`](catalog-pg-am.md "51.3. pg_am")| relation access methods  
[`pg_amop`](catalog-pg-amop.md "51.4. pg_amop")| access method operators  
[`pg_amproc`](catalog-pg-amproc.md "51.5. pg_amproc")| access method support functions  
[`pg_attrdef`](catalog-pg-attrdef.md "51.6. pg_attrdef")| column default values  
[`pg_attribute`](catalog-pg-attribute.md "51.7. pg_attribute")| table columns (“attributes”)  
[`pg_authid`](catalog-pg-authid.md "51.8. pg_authid")| authorization identifiers (roles)  
[`pg_auth_members`](catalog-pg-auth-members.md "51.9. pg_auth_members")| authorization identifier membership relationships  
[`pg_cast`](catalog-pg-cast.md "51.10. pg_cast")| casts (data type conversions)  
[`pg_class`](catalog-pg-class.md "51.11. pg_class")| tables, indexes, sequences, views (“relations”)  
[`pg_collation`](catalog-pg-collation.md "51.12. pg_collation")| collations (locale information)  
[`pg_constraint`](catalog-pg-constraint.md "51.13. pg_constraint")| check constraints, unique constraints, primary key constraints, foreign key constraints  
[`pg_conversion`](catalog-pg-conversion.md "51.14. pg_conversion")| encoding conversion information  
[`pg_database`](catalog-pg-database.md "51.15. pg_database")| databases within this database cluster  
[`pg_db_role_setting`](catalog-pg-db-role-setting.md "51.16. pg_db_role_setting")| per-role and per-database settings  
[`pg_default_acl`](catalog-pg-default-acl.md "51.17. pg_default_acl")| default privileges for object types  
[`pg_depend`](catalog-pg-depend.md "51.18. pg_depend")| dependencies between database objects  
[`pg_description`](catalog-pg-description.md "51.19. pg_description")| descriptions or comments on database objects  
[`pg_enum`](catalog-pg-enum.md "51.20. pg_enum")| enum label and value definitions  
[`pg_event_trigger`](catalog-pg-event-trigger.md "51.21. pg_event_trigger")| event triggers  
[`pg_extension`](catalog-pg-extension.md "51.22. pg_extension")| installed extensions  
[`pg_foreign_data_wrapper`](catalog-pg-foreign-data-wrapper.md "51.23. pg_foreign_data_wrapper")| foreign-data wrapper definitions  
[`pg_foreign_server`](catalog-pg-foreign-server.md "51.24. pg_foreign_server")| foreign server definitions  
[`pg_foreign_table`](catalog-pg-foreign-table.md "51.25. pg_foreign_table")| additional foreign table information  
[`pg_index`](catalog-pg-index.md "51.26. pg_index")| additional index information  
[`pg_inherits`](catalog-pg-inherits.md "51.27. pg_inherits")| table inheritance hierarchy  
[`pg_init_privs`](catalog-pg-init-privs.md "51.28. pg_init_privs")| object initial privileges  
[`pg_language`](catalog-pg-language.md "51.29. pg_language")| languages for writing functions  
[`pg_largeobject`](catalog-pg-largeobject.md "51.30. pg_largeobject")| data pages for large objects  
[`pg_largeobject_metadata`](catalog-pg-largeobject-metadata.md "51.31. pg_largeobject_metadata")| metadata for large objects  
[`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace")| schemas  
[`pg_opclass`](catalog-pg-opclass.md "51.33. pg_opclass")| access method operator classes  
[`pg_operator`](catalog-pg-operator.md "51.34. pg_operator")| operators  
[`pg_opfamily`](catalog-pg-opfamily.md "51.35. pg_opfamily")| access method operator families  
[`pg_parameter_acl`](catalog-pg-parameter-acl.md "51.36. pg_parameter_acl")| configuration parameters for which privileges have been granted  
[`pg_partitioned_table`](catalog-pg-partitioned-table.md "51.37. pg_partitioned_table")| information about partition key of tables  
[`pg_policy`](catalog-pg-policy.md "51.38. pg_policy")| row-security policies  
[`pg_proc`](catalog-pg-proc.md "51.39. pg_proc")| functions and procedures  
[`pg_publication`](catalog-pg-publication.md "51.40. pg_publication")| publications for logical replication  
[`pg_publication_namespace`](catalog-pg-publication-namespace.md "51.41. pg_publication_namespace")| schema to publication mapping  
[`pg_publication_rel`](catalog-pg-publication-rel.md "51.42. pg_publication_rel")| relation to publication mapping  
[`pg_range`](catalog-pg-range.md "51.43. pg_range")| information about range types  
[`pg_replication_origin`](catalog-pg-replication-origin.md "51.44. pg_replication_origin")| registered replication origins  
[`pg_rewrite`](catalog-pg-rewrite.md "51.45. pg_rewrite")| query rewrite rules  
[`pg_seclabel`](catalog-pg-seclabel.md "51.46. pg_seclabel")| security labels on database objects  
[`pg_sequence`](catalog-pg-sequence.md "51.47. pg_sequence")| information about sequences  
[`pg_shdepend`](catalog-pg-shdepend.md "51.48. pg_shdepend")| dependencies on shared objects  
[`pg_shdescription`](catalog-pg-shdescription.md "51.49. pg_shdescription")| comments on shared objects  
[`pg_shseclabel`](catalog-pg-shseclabel.md "51.50. pg_shseclabel")| security labels on shared database objects  
[`pg_statistic`](catalog-pg-statistic.md "51.51. pg_statistic")| planner statistics  
[`pg_statistic_ext`](catalog-pg-statistic-ext.md "51.52. pg_statistic_ext")| extended planner statistics (definition)  
[`pg_statistic_ext_data`](catalog-pg-statistic-ext-data.md "51.53. pg_statistic_ext_data")| extended planner statistics (built statistics)  
[`pg_subscription`](catalog-pg-subscription.md "51.54. pg_subscription")| logical replication subscriptions  
[`pg_subscription_rel`](catalog-pg-subscription-rel.md "51.55. pg_subscription_rel")| relation state for subscriptions  
[`pg_tablespace`](catalog-pg-tablespace.md "51.56. pg_tablespace")| tablespaces within this database cluster  
[`pg_transform`](catalog-pg-transform.md "51.57. pg_transform")| transforms (data type to procedural language conversions)  
[`pg_trigger`](catalog-pg-trigger.md "51.58. pg_trigger")| triggers  
[`pg_ts_config`](catalog-pg-ts-config.md "51.59. pg_ts_config")| text search configurations  
[`pg_ts_config_map`](catalog-pg-ts-config-map.md "51.60. pg_ts_config_map")| text search configurations' token mappings  
[`pg_ts_dict`](catalog-pg-ts-dict.md "51.61. pg_ts_dict")| text search dictionaries  
[`pg_ts_parser`](catalog-pg-ts-parser.md "51.62. pg_ts_parser")| text search parsers  
[`pg_ts_template`](catalog-pg-ts-template.md "51.63. pg_ts_template")| text search templates  
[`pg_type`](catalog-pg-type.md "51.64. pg_type")| data types  
[`pg_user_mapping`](catalog-pg-user-mapping.md "51.65. pg_user_mapping")| mappings of users to foreign servers  
  
  


* * *

[Prev](catalogs.md "Chapter 51. System Catalogs") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-aggregate.md "51.2. pg_aggregate")  
---|---|---  
Chapter 51. System Catalogs | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.2. `pg_aggregate`
