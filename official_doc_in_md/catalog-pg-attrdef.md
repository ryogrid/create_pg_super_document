51.6. `pg_attrdef`  
---  
[Prev](catalog-pg-amproc.md "51.5. pg_amproc") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-attribute.md "51.7. pg_attribute")  
  
* * *

## 51.6. `pg_attrdef` #

The catalog `pg_attrdef` stores column default values. The main information about columns is stored in [`pg_attribute`](catalog-pg-attribute.md "51.7. pg_attribute"). Only columns for which a default value has been explicitly set will have an entry here. 

**Table 51.6.`pg_attrdef` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`adrelid` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`oid`)  The table this column belongs to   
`adnum` `int2` (references [`pg_attribute`](catalog-pg-attribute.md "51.7. pg_attribute").`attnum`)  The number of the column   
`adbin` `pg_node_tree` The column default value, in `nodeToString()` representation. Use `pg_get_expr(adbin, adrelid)` to convert it to an SQL expression.   
  
  


* * *

[Prev](catalog-pg-amproc.md "51.5. pg_amproc") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-attribute.md "51.7. pg_attribute")  
---|---|---  
51.5. `pg_amproc` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.7. `pg_attribute`
