52.8. `pg_group`  
---  
[Prev](view-pg-file-settings.md "52.7. pg_file_settings") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-hba-file-rules.md "52.9. pg_hba_file_rules")  
  
* * *

## 52.8. `pg_group` #

The view `pg_group` exists for backwards compatibility: it emulates a catalog that existed in PostgreSQL before version 8.1. It shows the names and members of all roles that are marked as not `rolcanlogin`, which is an approximation to the set of roles that are being used as groups. 

**Table 52.8.`pg_group` Columns**

Column Type  Description   
---  
`groname` `name` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`rolname`)  Name of the group   
`grosysid` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  ID of this group   
`grolist` `oid[]` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  An array containing the IDs of the roles in this group   
  
  


* * *

[Prev](view-pg-file-settings.md "52.7. pg_file_settings") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-hba-file-rules.md "52.9. pg_hba_file_rules")  
---|---|---  
52.7. `pg_file_settings` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.9. `pg_hba_file_rules`
