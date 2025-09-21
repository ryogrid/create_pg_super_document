51.5. `pg_amproc`  
---  
[Prev](catalog-pg-amop.md "51.4. pg_amop") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-attrdef.md "51.6. pg_attrdef")  
  
* * *

## 51.5. `pg_amproc` #

The catalog `pg_amproc` stores information about support functions associated with access method operator families. There is one row for each support function belonging to an operator family. 

**Table 51.5.`pg_amproc` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`amprocfamily` `oid` (references [`pg_opfamily`](catalog-pg-opfamily.md "51.35. pg_opfamily").`oid`)  The operator family this entry is for   
`amproclefttype` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  Left-hand input data type of associated operator   
`amprocrighttype` `oid` (references [`pg_type`](catalog-pg-type.md "51.64. pg_type").`oid`)  Right-hand input data type of associated operator   
`amprocnum` `int2` Support function number   
`amproc` `regproc` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  OID of the function   
  
  


The usual interpretation of the `amproclefttype` and `amprocrighttype` fields is that they identify the left and right input types of the operator(s) that a particular support function supports. For some access methods these match the input data type(s) of the support function itself, for others not. There is a notion of “default” support functions for an index, which are those with `amproclefttype` and `amprocrighttype` both equal to the index operator class's `opcintype`. 

* * *

[Prev](catalog-pg-amop.md "51.4. pg_amop") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-attrdef.md "51.6. pg_attrdef")  
---|---|---  
51.4. `pg_amop` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.6. `pg_attrdef`
