24.2. Routine Reindexing  
---  
[Prev](routine-vacuuming.md "24.1. Routine Vacuuming") | [Up](maintenance.md "Chapter 24. Routine Database Maintenance Tasks")| Chapter 24. Routine Database Maintenance Tasks| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](logfile-maintenance.md "24.3. Log File Maintenance")  
  
* * *

## 24.2. Routine Reindexing #

In some situations it is worthwhile to rebuild indexes periodically with the [REINDEX](sql-reindex.md "REINDEX") command or a series of individual rebuilding steps. 

B-tree index pages that have become completely empty are reclaimed for re-use. However, there is still a possibility of inefficient use of space: if all but a few index keys on a page have been deleted, the page remains allocated. Therefore, a usage pattern in which most, but not all, keys in each range are eventually deleted will see poor use of space. For such usage patterns, periodic reindexing is recommended. 

The potential for bloat in non-B-tree indexes has not been well researched. It is a good idea to periodically monitor the index's physical size when using any non-B-tree index type. 

Also, for B-tree indexes, a freshly-constructed index is slightly faster to access than one that has been updated many times because logically adjacent pages are usually also physically adjacent in a newly built index. (This consideration does not apply to non-B-tree indexes.) It might be worthwhile to reindex periodically just to improve access speed. 

[REINDEX](sql-reindex.md "REINDEX") can be used safely and easily in all cases. This command requires an `ACCESS EXCLUSIVE` lock by default, hence it is often preferable to execute it with its `CONCURRENTLY` option, which requires only a `SHARE UPDATE EXCLUSIVE` lock. 

* * *

[Prev](routine-vacuuming.md "24.1. Routine Vacuuming") | [Up](maintenance.md "Chapter 24. Routine Database Maintenance Tasks")|  [Next](logfile-maintenance.md "24.3. Log File Maintenance")  
---|---|---  
24.1. Routine Vacuuming | [Home](index.md "PostgreSQL 17.5 Documentation")|  24.3. Log File Maintenance
