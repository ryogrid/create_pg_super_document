35.2. Data Types  
---  
[Prev](infoschema-schema.md "35.1. The Schema") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-information-schema-catalog-name.md "35.3. information_schema_catalog_name")  
  
* * *

## 35.2. Data Types #

The columns of the information schema views use special data types that are defined in the information schema. These are defined as simple domains over ordinary built-in types. You should not use these types for work outside the information schema, but your applications must be prepared for them if they select from the information schema. 

These types are: 

`cardinal_number`
    

A nonnegative integer. 

`character_data`
    

A character string (without specific maximum length). 

`sql_identifier`
    

A character string. This type is used for SQL identifiers, the type `character_data` is used for any other kind of text data. 

`time_stamp`
    

A domain over the type `timestamp with time zone`

`yes_or_no`
    

A character string domain that contains either `YES` or `NO`. This is used to represent Boolean (true/false) data in the information schema. (The information schema was invented before the type `boolean` was added to the SQL standard, so this convention is necessary to keep the information schema backward compatible.) 

Every column in the information schema has one of these five types. 

* * *

[Prev](infoschema-schema.md "35.1. The Schema") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-information-schema-catalog-name.md "35.3. information_schema_catalog_name")  
---|---|---  
35.1. The Schema | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.3. `information_schema_catalog_name`
