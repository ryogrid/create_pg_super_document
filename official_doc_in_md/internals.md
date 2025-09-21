Part VII. Internals  
---  
[Prev](app-postgres.md "postgres") | [Up](index.md "PostgreSQL 17.5 Documentation")| PostgreSQL 17.5 Documentation| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](overview.md "Chapter 50. Overview of PostgreSQL Internals")  
  
* * *

# Part VII. Internals

This part contains assorted information that might be of use to PostgreSQL developers. 

**Table of Contents**

[50\. Overview of PostgreSQL Internals](overview.md)
    

[50.1. The Path of a Query](query-path.md)
[50.2. How Connections Are Established](connect-estab.md)
[50.3. The Parser Stage](parser-stage.md)
[50.4. The PostgreSQL Rule System](rule-system.md)
[50.5. Planner/Optimizer](planner-optimizer.md)
[50.6. Executor](executor.md)
[51\. System Catalogs](catalogs.md)
    

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
[52\. System Views](views.md)
    

[52.1. Overview](views-overview.md)
[52.2. `pg_available_extensions`](view-pg-available-extensions.md)
[52.3. `pg_available_extension_versions`](view-pg-available-extension-versions.md)
[52.4. `pg_backend_memory_contexts`](view-pg-backend-memory-contexts.md)
[52.5. `pg_config`](view-pg-config.md)
[52.6. `pg_cursors`](view-pg-cursors.md)
[52.7. `pg_file_settings`](view-pg-file-settings.md)
[52.8. `pg_group`](view-pg-group.md)
[52.9. `pg_hba_file_rules`](view-pg-hba-file-rules.md)
[52.10. `pg_ident_file_mappings`](view-pg-ident-file-mappings.md)
[52.11. `pg_indexes`](view-pg-indexes.md)
[52.12. `pg_locks`](view-pg-locks.md)
[52.13. `pg_matviews`](view-pg-matviews.md)
[52.14. `pg_policies`](view-pg-policies.md)
[52.15. `pg_prepared_statements`](view-pg-prepared-statements.md)
[52.16. `pg_prepared_xacts`](view-pg-prepared-xacts.md)
[52.17. `pg_publication_tables`](view-pg-publication-tables.md)
[52.18. `pg_replication_origin_status`](view-pg-replication-origin-status.md)
[52.19. `pg_replication_slots`](view-pg-replication-slots.md)
[52.20. `pg_roles`](view-pg-roles.md)
[52.21. `pg_rules`](view-pg-rules.md)
[52.22. `pg_seclabels`](view-pg-seclabels.md)
[52.23. `pg_sequences`](view-pg-sequences.md)
[52.24. `pg_settings`](view-pg-settings.md)
[52.25. `pg_shadow`](view-pg-shadow.md)
[52.26. `pg_shmem_allocations`](view-pg-shmem-allocations.md)
[52.27. `pg_stats`](view-pg-stats.md)
[52.28. `pg_stats_ext`](view-pg-stats-ext.md)
[52.29. `pg_stats_ext_exprs`](view-pg-stats-ext-exprs.md)
[52.30. `pg_tables`](view-pg-tables.md)
[52.31. `pg_timezone_abbrevs`](view-pg-timezone-abbrevs.md)
[52.32. `pg_timezone_names`](view-pg-timezone-names.md)
[52.33. `pg_user`](view-pg-user.md)
[52.34. `pg_user_mappings`](view-pg-user-mappings.md)
[52.35. `pg_views`](view-pg-views.md)
[52.36. `pg_wait_events`](view-pg-wait-events.md)
[53\. Frontend/Backend Protocol](protocol.md)
    

[53.1. Overview](protocol-overview.md)
[53.2. Message Flow](protocol-flow.md)
[53.3. SASL Authentication](sasl-authentication.md)
[53.4. Streaming Replication Protocol](protocol-replication.md)
[53.5. Logical Streaming Replication Protocol](protocol-logical-replication.md)
[53.6. Message Data Types](protocol-message-types.md)
[53.7. Message Formats](protocol-message-formats.md)
[53.8. Error and Notice Message Fields](protocol-error-fields.md)
[53.9. Logical Replication Message Formats](protocol-logicalrep-message-formats.md)
[53.10. Summary of Changes since Protocol 2.0](protocol-changes.md)
[54\. PostgreSQL Coding Conventions](source.md)
    

[54.1. Formatting](source-format.md)
[54.2. Reporting Errors Within the Server](error-message-reporting.md)
[54.3. Error Message Style Guide](error-style-guide.md)
[54.4. Miscellaneous Coding Conventions](source-conventions.md)
[55\. Native Language Support](nls.md)
    

[55.1. For the Translator](nls-translator.md)
[55.2. For the Programmer](nls-programmer.md)
[56\. Writing a Procedural Language Handler](plhandler.md)
[57\. Writing a Foreign Data Wrapper](fdwhandler.md)
    

[57.1. Foreign Data Wrapper Functions](fdw-functions.md)
[57.2. Foreign Data Wrapper Callback Routines](fdw-callbacks.md)
[57.3. Foreign Data Wrapper Helper Functions](fdw-helpers.md)
[57.4. Foreign Data Wrapper Query Planning](fdw-planning.md)
[57.5. Row Locking in Foreign Data Wrappers](fdw-row-locking.md)
[58\. Writing a Table Sampling Method](tablesample-method.md)
    

[58.1. Sampling Method Support Functions](tablesample-support-functions.md)
[59\. Writing a Custom Scan Provider](custom-scan.md)
    

[59.1. Creating Custom Scan Paths](custom-scan-path.md)
[59.2. Creating Custom Scan Plans](custom-scan-plan.md)
[59.3. Executing Custom Scans](custom-scan-execution.md)
[60\. Genetic Query Optimizer](geqo.md)
    

[60.1. Query Handling as a Complex Optimization Problem](geqo-intro.md)
[60.2. Genetic Algorithms](geqo-intro2.md)
[60.3. Genetic Query Optimization (GEQO) in PostgreSQL](geqo-pg-intro.md)
[60.4. Further Reading](geqo-biblio.md)
[61\. Table Access Method Interface Definition](tableam.md)
[62\. Index Access Method Interface Definition](indexam.md)
    

[62.1. Basic API Structure for Indexes](index-api.md)
[62.2. Index Access Method Functions](index-functions.md)
[62.3. Index Scanning](index-scanning.md)
[62.4. Index Locking Considerations](index-locking.md)
[62.5. Index Uniqueness Checks](index-unique-checks.md)
[62.6. Index Cost Estimation Functions](index-cost-estimation.md)
[63\. Write Ahead Logging for Extensions](wal-for-extensions.md)
    

[63.1. Generic WAL Records](generic-wal.md)
[63.2. Custom WAL Resource Managers](custom-rmgr.md)
[64\. Built-in Index Access Methods](indextypes.md)
    

[64.1. B-Tree Indexes](btree.md)
[64.2. GiST Indexes](gist.md)
[64.3. SP-GiST Indexes](spgist.md)
[64.4. GIN Indexes](gin.md)
[64.5. BRIN Indexes](brin.md)
[64.6. Hash Indexes](hash-index.md)
[65\. Database Physical Storage](storage.md)
    

[65.1. Database File Layout](storage-file-layout.md)
[65.2. TOAST](storage-toast.md)
[65.3. Free Space Map](storage-fsm.md)
[65.4. Visibility Map](storage-vm.md)
[65.5. The Initialization Fork](storage-init.md)
[65.6. Database Page Layout](storage-page-layout.md)
[65.7. Heap-Only Tuples (HOT)](storage-hot.md)
[66\. Transaction Processing](transactions.md)
    

[66.1. Transactions and Identifiers](transaction-id.md)
[66.2. Transactions and Locking](xact-locking.md)
[66.3. Subtransactions](subxacts.md)
[66.4. Two-Phase Transactions](two-phase.md)
[67\. System Catalog Declarations and Initial Contents](bki.md)
    

[67.1. System Catalog Declaration Rules](system-catalog-declarations.md)
[67.2. System Catalog Initial Data](system-catalog-initial-data.md)
[67.3. BKI File Format](bki-format.md)
[67.4. BKI Commands](bki-commands.md)
[67.5. Structure of the Bootstrap BKI File](bki-structure.md)
[67.6. BKI Example](bki-example.md)
[68\. How the Planner Uses Statistics](planner-stats-details.md)
    

[68.1. Row Estimation Examples](row-estimation-examples.md)
[68.2. Multivariate Statistics Examples](multivariate-statistics-examples.md)
[68.3. Planner Statistics and Security](planner-stats-security.md)
[69\. Backup Manifest Format](backup-manifest-format.md)
    

[69.1. Backup Manifest Top-level Object](backup-manifest-toplevel.md)
[69.2. Backup Manifest File Object](backup-manifest-files.md)
[69.3. Backup Manifest WAL Range Object](backup-manifest-wal-ranges.md)

* * *

[Prev](app-postgres.md "postgres") | [Up](index.md "PostgreSQL 17.5 Documentation")|  [Next](overview.md "Chapter 50. Overview of PostgreSQL Internals")  
---|---|---  
postgres | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 50. Overview of PostgreSQL Internals
