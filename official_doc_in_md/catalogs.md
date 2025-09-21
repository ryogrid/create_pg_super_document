Chapter 51. System Catalogs  
---  
[Prev](executor.md "50.6. Executor") | [Up](internals.md "Part VII. Internals")| Part VII. Internals| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalogs-overview.md "51.1. Overview")  
  
* * *

## Chapter 51. System Catalogs

**Table of Contents**

[51.1. Overview](catalogs-overview.md)
[51.2. `pg_aggregate`](catalog-pg-aggregate.md)
[51.3. `pg_am`](catalog-pg-am.md)
[51.4. `pg_amop`](catalog-pg-amop.md)
[51.5. `pg_amproc`](catalog-pg-amproc.md)
[51.6. `pg_attrdef`](catalog-pg-attrdef.md)
[51.7. `pg_attribute`](catalog-pg-attribute.md)
[51.8. `pg_authid`](catalog-pg-authid.md)
[51.9. `pg_auth_members`](catalog-pg-auth-members.md)
[51.10. `pg_cast`](catalog-pg-cast.md)
[51.11. `pg_class`](catalog-pg-class.md)
[51.12. `pg_collation`](catalog-pg-collation.md)
[51.13. `pg_constraint`](catalog-pg-constraint.md)
[51.14. `pg_conversion`](catalog-pg-conversion.md)
[51.15. `pg_database`](catalog-pg-database.md)
[51.16. `pg_db_role_setting`](catalog-pg-db-role-setting.md)
[51.17. `pg_default_acl`](catalog-pg-default-acl.md)
[51.18. `pg_depend`](catalog-pg-depend.md)
[51.19. `pg_description`](catalog-pg-description.md)
[51.20. `pg_enum`](catalog-pg-enum.md)
[51.21. `pg_event_trigger`](catalog-pg-event-trigger.md)
[51.22. `pg_extension`](catalog-pg-extension.md)
[51.23. `pg_foreign_data_wrapper`](catalog-pg-foreign-data-wrapper.md)
[51.24. `pg_foreign_server`](catalog-pg-foreign-server.md)
[51.25. `pg_foreign_table`](catalog-pg-foreign-table.md)
[51.26. `pg_index`](catalog-pg-index.md)
[51.27. `pg_inherits`](catalog-pg-inherits.md)
[51.28. `pg_init_privs`](catalog-pg-init-privs.md)
[51.29. `pg_language`](catalog-pg-language.md)
[51.30. `pg_largeobject`](catalog-pg-largeobject.md)
[51.31. `pg_largeobject_metadata`](catalog-pg-largeobject-metadata.md)
[51.32. `pg_namespace`](catalog-pg-namespace.md)
[51.33. `pg_opclass`](catalog-pg-opclass.md)
[51.34. `pg_operator`](catalog-pg-operator.md)
[51.35. `pg_opfamily`](catalog-pg-opfamily.md)
[51.36. `pg_parameter_acl`](catalog-pg-parameter-acl.md)
[51.37. `pg_partitioned_table`](catalog-pg-partitioned-table.md)
[51.38. `pg_policy`](catalog-pg-policy.md)
[51.39. `pg_proc`](catalog-pg-proc.md)
[51.40. `pg_publication`](catalog-pg-publication.md)
[51.41. `pg_publication_namespace`](catalog-pg-publication-namespace.md)
[51.42. `pg_publication_rel`](catalog-pg-publication-rel.md)
[51.43. `pg_range`](catalog-pg-range.md)
[51.44. `pg_replication_origin`](catalog-pg-replication-origin.md)
[51.45. `pg_rewrite`](catalog-pg-rewrite.md)
[51.46. `pg_seclabel`](catalog-pg-seclabel.md)
[51.47. `pg_sequence`](catalog-pg-sequence.md)
[51.48. `pg_shdepend`](catalog-pg-shdepend.md)
[51.49. `pg_shdescription`](catalog-pg-shdescription.md)
[51.50. `pg_shseclabel`](catalog-pg-shseclabel.md)
[51.51. `pg_statistic`](catalog-pg-statistic.md)
[51.52. `pg_statistic_ext`](catalog-pg-statistic-ext.md)
[51.53. `pg_statistic_ext_data`](catalog-pg-statistic-ext-data.md)
[51.54. `pg_subscription`](catalog-pg-subscription.md)
[51.55. `pg_subscription_rel`](catalog-pg-subscription-rel.md)
[51.56. `pg_tablespace`](catalog-pg-tablespace.md)
[51.57. `pg_transform`](catalog-pg-transform.md)
[51.58. `pg_trigger`](catalog-pg-trigger.md)
[51.59. `pg_ts_config`](catalog-pg-ts-config.md)
[51.60. `pg_ts_config_map`](catalog-pg-ts-config-map.md)
[51.61. `pg_ts_dict`](catalog-pg-ts-dict.md)
[51.62. `pg_ts_parser`](catalog-pg-ts-parser.md)
[51.63. `pg_ts_template`](catalog-pg-ts-template.md)
[51.64. `pg_type`](catalog-pg-type.md)
[51.65. `pg_user_mapping`](catalog-pg-user-mapping.md)

The system catalogs are the place where a relational database management system stores schema metadata, such as information about tables and columns, and internal bookkeeping information. PostgreSQL's system catalogs are regular tables. You can drop and recreate the tables, add columns, insert and update values, and severely mess up your system that way. Normally, one should not change the system catalogs by hand, there are normally SQL commands to do that. (For example, `CREATE DATABASE` inserts a row into the `pg_database` catalog — and actually creates the database on disk.) There are some exceptions for particularly esoteric operations, but many of those have been made available as SQL commands over time, and so the need for direct manipulation of the system catalogs is ever decreasing. 

* * *

[Prev](executor.md "50.6. Executor") | [Up](internals.md "Part VII. Internals")|  [Next](catalogs-overview.md "51.1. Overview")  
---|---|---  
50.6. Executor | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.1. Overview
