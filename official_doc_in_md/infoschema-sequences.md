35.47. `sequences`  
---  
[Prev](infoschema-schemata.md "35.46. schemata") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-sql-features.md "35.48. sql_features")  
  
* * *

## 35.47. `sequences` #

The view `sequences` contains all sequences defined in the current database. Only those sequences are shown that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.45.`sequences` Columns**

Column Type  Description   
---  
`sequence_catalog` `sql_identifier` Name of the database that contains the sequence (always the current database)   
`sequence_schema` `sql_identifier` Name of the schema that contains the sequence   
`sequence_name` `sql_identifier` Name of the sequence   
`data_type` `character_data` The data type of the sequence.   
`numeric_precision` `cardinal_number` This column contains the (declared or implicit) precision of the sequence data type (see above). The precision indicates the number of significant digits. It can be expressed in decimal (base 10) or binary (base 2) terms, as specified in the column `numeric_precision_radix`.   
`numeric_precision_radix` `cardinal_number` This column indicates in which base the values in the columns `numeric_precision` and `numeric_scale` are expressed. The value is either 2 or 10.   
`numeric_scale` `cardinal_number` This column contains the (declared or implicit) scale of the sequence data type (see above). The scale indicates the number of significant digits to the right of the decimal point. It can be expressed in decimal (base 10) or binary (base 2) terms, as specified in the column `numeric_precision_radix`.   
`start_value` `character_data` The start value of the sequence   
`minimum_value` `character_data` The minimum value of the sequence   
`maximum_value` `character_data` The maximum value of the sequence   
`increment` `character_data` The increment of the sequence   
`cycle_option` `yes_or_no` `YES` if the sequence cycles, else `NO`  
  
  


Note that in accordance with the SQL standard, the start, minimum, maximum, and increment values are returned as character strings. 

* * *

[Prev](infoschema-schemata.md "35.46. schemata") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-sql-features.md "35.48. sql_features")  
---|---|---  
35.46. `schemata` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.48. `sql_features`
