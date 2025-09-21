51.35. `pg_opfamily`  
---  
[Prev](catalog-pg-operator.md "51.34. pg_operator") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-parameter-acl.md "51.36. pg_parameter_acl")  
  
* * *

## 51.35. `pg_opfamily` #

The catalog `pg_opfamily` defines operator families. Each operator family is a collection of operators and associated support routines that implement the semantics specified for a particular index access method. Furthermore, the operators in a family are all “compatible”, in a way that is specified by the access method. The operator family concept allows cross-data-type operators to be used with indexes and to be reasoned about using knowledge of access method semantics. 

Operator families are described at length in [Section 36.16](xindex.md "36.16. Interfacing Extensions to Indexes"). 

**Table 51.35.`pg_opfamily` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`opfmethod` `oid` (references [`pg_am`](catalog-pg-am.md "51.3. pg_am").`oid`)  Index access method operator family is for   
`opfname` `name` Name of this operator family   
`opfnamespace` `oid` (references [`pg_namespace`](catalog-pg-namespace.md "51.32. pg_namespace").`oid`)  Namespace of this operator family   
`opfowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the operator family   
  
  


The majority of the information defining an operator family is not in its `pg_opfamily` row, but in the associated rows in [`pg_amop`](catalog-pg-amop.md "51.4. pg_amop"), [`pg_amproc`](catalog-pg-amproc.md "51.5. pg_amproc"), and [`pg_opclass`](catalog-pg-opclass.md "51.33. pg_opclass"). 

* * *

[Prev](catalog-pg-operator.md "51.34. pg_operator") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-parameter-acl.md "51.36. pg_parameter_acl")  
---|---|---  
51.34. `pg_operator` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.36. `pg_parameter_acl`
