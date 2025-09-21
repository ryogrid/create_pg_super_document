52.20. `pg_roles`  
---  
[Prev](view-pg-replication-slots.md "52.19. pg_replication_slots") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-rules.md "52.21. pg_rules")  
  
* * *

## 52.20. `pg_roles` #

The view `pg_roles` provides access to information about database roles. This is simply a publicly readable view of [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid") that blanks out the password field. 

**Table 52.20.`pg_roles` Columns**

Column Type  Description   
---  
`rolname` `name` Role name   
`rolsuper` `bool` Role has superuser privileges   
`rolinherit` `bool` Role automatically inherits privileges of roles it is a member of   
`rolcreaterole` `bool` Role can create more roles   
`rolcreatedb` `bool` Role can create databases   
`rolcanlogin` `bool` Role can log in. That is, this role can be given as the initial session authorization identifier   
`rolreplication` `bool` Role is a replication role. A replication role can initiate replication connections and create and drop replication slots.   
`rolconnlimit` `int4` For roles that can log in, this sets maximum number of concurrent connections this role can make. -1 means no limit.   
`rolpassword` `text` Not the password (always reads as `********`)   
`rolvaliduntil` `timestamptz` Password expiry time (only used for password authentication); null if no expiration   
`rolbypassrls` `bool` Role bypasses every row-level security policy, see [Section 5.9](ddl-rowsecurity.md "5.9. Row Security Policies") for more information.   
`rolconfig` `text[]` Role-specific defaults for run-time configuration variables   
`oid` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  ID of role   
  
  


* * *

[Prev](view-pg-replication-slots.md "52.19. pg_replication_slots") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-rules.md "52.21. pg_rules")  
---|---|---  
52.19. `pg_replication_slots` | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.21. `pg_rules`
