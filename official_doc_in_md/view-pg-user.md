52.33. `pg_user`  
---  
[Prev](view-pg-timezone-names.md "52.32. pg_timezone_names") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-user-mappings.md "52.34. pg_user_mappings")  
  
* * *

## 52.33. `pg_user` #

The view `pg_user` provides access to information about database users. This is simply a publicly readable view of [`pg_shadow`](view-pg-shadow.md "52.25. pg_shadow") that blanks out the password field. 

**Table 52.33.`pg_user` Columns**

Column Type  Description   
---  
`usename` `name` User name   
`usesysid` `oid` ID of this user   
`usecreatedb` `bool` User can create databases   
`usesuper` `bool` User is a superuser   
`userepl` `bool` User can initiate streaming replication and put the system in and out of backup mode.   
`usebypassrls` `bool` User bypasses every row-level security policy, see [Section 5.9](ddl-rowsecurity.md "5.9. Row Security Policies") for more information.   
`passwd` `text` Not the password (always reads as `********`)   
`valuntil` `timestamptz` Password expiry time (only used for password authentication)   
`useconfig` `text[]` Session defaults for run-time configuration variables   
  
  


* * *

[Prev](view-pg-timezone-names.md "52.32. pg_timezone_names") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-user-mappings.md "52.34. pg_user_mappings")  
---|---|---  
52.32. `pg_timezone_names` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.34. `pg_user_mappings`
