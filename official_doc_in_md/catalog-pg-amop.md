51.4. `pg_amop`  
---  
[Prev](catalog-pg-am.md "51.3. pg_am") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-amproc.md "51.5. pg_amproc")  
  
* * *

## 51.4. `pg_amop` #

The catalog `pg_amop` stores information about operators associated with access method operator families. There is one row for each operator that is a member of an operator family. A family member can be either a _search_ operator or an _ordering_ operator. An operator can appear in more than one family, but cannot appear in more than one search position nor more than one ordering position within a family. (It is allowed, though unlikely, for an operator to be used for both search and ordering purposes.) 

**Table 51.4.`pg_amop` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`amopfamily` `oid` (references [`pg_opfamily`](catalog-pg-opfamily.md "51.35. pg_opfamily").`oid`)  The operator family this entry is for   
`amoplefttype` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  Left-hand input data type of operator   
`amoprighttype` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  Right-hand input data type of operator   
`amopstrategy` `int2` Operator strategy number   
`amoppurpose` `char` Operator purpose, either `s` for search or `o` for ordering   
`amopopr` `oid` (references [`pg_operator`](catalog-pg-operator.md "51.34. pg_operator").`oid`)  OID of the operator   
`amopmethod` `oid` (references [`pg_am`](catalog-pg-am.md "51.3. pg_am").`oid`)  Index access method operator family is for   
`amopsortfamily` `oid` (references [`pg_opfamily`](catalog-pg-opfamily.md "51.35. pg_opfamily").`oid`)  The B-tree operator family this entry sorts according to, if an ordering operator; zero if a search operator   
  
  


A “search” operator entry indicates that an index of this operator family can be searched to find all rows satisfying `WHERE` _`indexed_column`_ _`operator`_ _`constant`_. Obviously, such an operator must return `boolean`, and its left-hand input type must match the index's column data type. 

An “ordering” operator entry indicates that an index of this operator family can be scanned to return rows in the order represented by `ORDER BY` _`indexed_column`_ _`operator`_ _`constant`_. Such an operator could return any sortable data type, though again its left-hand input type must match the index's column data type. The exact semantics of the `ORDER BY` are specified by the `amopsortfamily` column, which must reference a B-tree operator family for the operator's result type. 

### Note

At present, it's assumed that the sort order for an ordering operator is the default for the referenced operator family, i.e., `ASC NULLS LAST`. This might someday be relaxed by adding additional columns to specify sort options explicitly. 

An entry's `amopmethod` must match the `opfmethod` of its containing operator family (including `amopmethod` here is an intentional denormalization of the catalog structure for performance reasons). Also, `amoplefttype` and `amoprighttype` must match the `oprleft` and `oprright` fields of the referenced [`pg_operator`](catalog-pg-operator.md "51.34. pg_operator") entry. 

* * *

[Prev](catalog-pg-am.md "51.3. pg_am") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-amproc.md "51.5. pg_amproc")  
---|---|---  
51.3. `pg_am` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.5. `pg_amproc`
