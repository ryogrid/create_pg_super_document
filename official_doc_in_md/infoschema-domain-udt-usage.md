35.22. `domain_udt_usage`  
---  
[Prev](infoschema-domain-constraints.md "35.21. domain_constraints") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](infoschema-domains.md "35.23. domains")  
  
* * *

## 35.22. `domain_udt_usage` #

The view `domain_udt_usage` identifies all domains that are based on data types owned by a currently enabled role. Note that in PostgreSQL, built-in data types behave like user-defined types, so they are included here as well. 

**Table 35.20.`domain_udt_usage` Columns**

Column Type  Description   
---  
`udt_catalog` `sql_identifier` Name of the database that the domain data type is defined in (always the current database)   
`udt_schema` `sql_identifier` Name of the schema that the domain data type is defined in   
`udt_name` `sql_identifier` Name of the domain data type   
`domain_catalog` `sql_identifier` Name of the database that contains the domain (always the current database)   
`domain_schema` `sql_identifier` Name of the schema that contains the domain   
`domain_name` `sql_identifier` Name of the domain   
  
  


* * *

[Prev](infoschema-domain-constraints.md "35.21. domain_constraints") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](infoschema-domains.md "35.23. domains")  
---|---|---  
35.21. `domain_constraints` | [Home](index.md "PostgreSQL 17.5 Documentation")|  35.23. `domains`
