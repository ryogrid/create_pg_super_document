51.34. `pg_operator`  
---  
[Prev](catalog-pg-opclass.md "51.33. pg_opclass") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-opfamily.md "51.35. pg_opfamily")  
  
* * *

## 51.34. `pg_operator` #

The catalog `pg_operator` stores information about operators. See [CREATE OPERATOR](sql-createoperator.md "CREATE OPERATOR") and [Section 36.14](xoper.md "36.14. User-Defined Operators") for more information. 

**Table 51.34.`pg_operator` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`oprname` `name` Name of the operator   
`oprnamespace` `oid` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`oid`)  The OID of the namespace that contains this operator   
`oprowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the operator   
`oprkind` `char` `b` = infix operator (“both”), or `l` = prefix operator (“left”)   
`oprcanmerge` `bool` This operator supports merge joins   
`oprcanhash` `bool` This operator supports hash joins   
`oprleft` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  Type of the left operand (zero for a prefix operator)   
`oprright` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  Type of the right operand   
`oprresult` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  Type of the result (zero for a not-yet-defined “shell” operator)   
`oprcom` `oid` (references [`pg_operator`](catalog-pg-operator.md "51.34. pg_operator").`oid`)  Commutator of this operator (zero if none)   
`oprnegate` `oid` (references [`pg_operator`](catalog-pg-operator.md "51.34. pg_operator").`oid`)  Negator of this operator (zero if none)   
`oprcode` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  Function that implements this operator (zero for a not-yet-defined “shell” operator)   
`oprrest` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  Restriction selectivity estimation function for this operator (zero if none)   
`oprjoin` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  Join selectivity estimation function for this operator (zero if none)   
  
  


* * *

[Prev](catalog-pg-opclass.md "51.33. pg_opclass") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-opfamily.md "51.35. pg_opfamily")  
---|---|---  
51.33. `pg_opclass` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.35. `pg_opfamily`
