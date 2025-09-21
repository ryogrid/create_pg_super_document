Chapter 52. System Views  
---  
[Prev](catalog-pg-user-mapping.md "51.65. pg_user_mapping") | [Up](internals.md "Part VII. Internals")| Part VII. Internals| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](views-overview.md "52.1. Overview")  
  
* * *

## Chapter 52. System Views

**Table of Contents**

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

In addition to the system catalogs, PostgreSQL provides a number of built-in views. Some system views provide convenient access to some commonly used queries on the system catalogs. Other views provide access to internal server state. 

The information schema ([Chapter 35](information-schema.md "Chapter 35. The Information Schema")) provides an alternative set of views which overlap the functionality of the system views. Since the information schema is SQL-standard whereas the views described here are PostgreSQL-specific, it's usually better to use the information schema if it provides all the information you need. 

[Table 52.1](views-overview.md#VIEW-TABLE "Table 52.1. System Views") lists the system views described here. More detailed documentation of each view follows below. There are some additional views that provide access to accumulated statistics; they are described in [Table 27.2](monitoring-stats.md#MONITORING-STATS-VIEWS-TABLE "Table 27.2. Collected Statistics Views"). 

* * *

[Prev](catalog-pg-user-mapping.md "51.65. pg_user_mapping") | [Up](internals.md "Part VII. Internals")|  [Next](views-overview.md "52.1. Overview")  
---|---|---  
51.65. `pg_user_mapping` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.1. Overview
