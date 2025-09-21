52.1. Overview  
---  
[Prev](views.md "Chapter 52. System Views") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-available-extensions.md "52.2. pg_available_extensions")  
  
* * *

## 52.1. Overview #

[Table 52.1](views-overview.md#VIEW-TABLE "Table 52.1. System Views") lists the system views. More detailed documentation of each catalog follows below. Except where noted, all the views described here are read-only. 

**Table 52.1. System Views**

View Name| Purpose  
---|---  
[`pg_available_extensions`](view-pg-available-extensions.md "52.2. pg_available_extensions")| available extensions  
[`pg_available_extension_versions`](view-pg-available-extension-versions.md "52.3. pg_available_extension_versions")| available versions of extensions  
[`pg_backend_memory_contexts`](view-pg-backend-memory-contexts.md "52.4. pg_backend_memory_contexts")| backend memory contexts  
[`pg_config`](view-pg-config.md "52.5. pg_config")| compile-time configuration parameters  
[`pg_cursors`](view-pg-cursors.md "52.6. pg_cursors")| open cursors  
[`pg_file_settings`](view-pg-file-settings.md "52.7. pg_file_settings")| summary of configuration file contents  
[`pg_group`](view-pg-group.md "52.8. pg_group")| groups of database users  
[`pg_hba_file_rules`](view-pg-hba-file-rules.md "52.9. pg_hba_file_rules")| summary of client authentication configuration file contents  
[`pg_ident_file_mappings`](view-pg-ident-file-mappings.md "52.10. pg_ident_file_mappings")| summary of client user name mapping configuration file contents  
[`pg_indexes`](view-pg-indexes.md "52.11. pg_indexes")| indexes  
[`pg_locks`](view-pg-locks.md "52.12. pg_locks")| locks currently held or awaited  
[`pg_matviews`](view-pg-matviews.md "52.13. pg_matviews")| materialized views  
[`pg_policies`](view-pg-policies.md "52.14. pg_policies")| policies  
[`pg_prepared_statements`](view-pg-prepared-statements.md "52.15. pg_prepared_statements")| prepared statements  
[`pg_prepared_xacts`](view-pg-prepared-xacts.md "52.16. pg_prepared_xacts")| prepared transactions  
[`pg_publication_tables`](view-pg-publication-tables.md "52.17. pg_publication_tables")| publications and information of their associated tables  
[`pg_replication_origin_status`](view-pg-replication-origin-status.md "52.18. pg_replication_origin_status")| information about replication origins, including replication progress  
[`pg_replication_slots`](view-pg-replication-slots.md "52.19. pg_replication_slots")| replication slot information  
[`pg_roles`](view-pg-roles.md "52.20. pg_roles")| database roles  
[`pg_rules`](view-pg-rules.md "52.21. pg_rules")| rules  
[`pg_seclabels`](view-pg-seclabels.md "52.22. pg_seclabels")| security labels  
[`pg_sequences`](view-pg-sequences.md "52.23. pg_sequences")| sequences  
[`pg_settings`](view-pg-settings.md "52.24. pg_settings")| parameter settings  
[`pg_shadow`](view-pg-shadow.md "52.25. pg_shadow")| database users  
[`pg_shmem_allocations`](view-pg-shmem-allocations.md "52.26. pg_shmem_allocations")| shared memory allocations  
[`pg_stats`](view-pg-stats.md "52.27. pg_stats")| planner statistics  
[`pg_stats_ext`](view-pg-stats-ext.md "52.28. pg_stats_ext")| extended planner statistics  
[`pg_stats_ext_exprs`](view-pg-stats-ext-exprs.md "52.29. pg_stats_ext_exprs")| extended planner statistics for expressions  
[`pg_tables`](view-pg-tables.md "52.30. pg_tables")| tables  
[`pg_timezone_abbrevs`](view-pg-timezone-abbrevs.md "52.31. pg_timezone_abbrevs")| time zone abbreviations  
[`pg_timezone_names`](view-pg-timezone-names.md "52.32. pg_timezone_names")| time zone names  
[`pg_user`](view-pg-user.md "52.33. pg_user")| database users  
[`pg_user_mappings`](view-pg-user-mappings.md "52.34. pg_user_mappings")| user mappings  
[`pg_views`](view-pg-views.md "52.35. pg_views")| views  
[`pg_wait_events`](view-pg-wait-events.md "52.36. pg_wait_events")| wait events  
  
  


* * *

[Prev](views.md "Chapter 52. System Views") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-available-extensions.md "52.2. pg_available_extensions")  
---|---|---  
Chapter 52. System Views | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.2. `pg_available_extensions`
