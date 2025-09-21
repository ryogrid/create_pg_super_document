35.66. `views`  
---  
[Prev](infoschema-view-table-usage.md "35.65. view_table_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")| Chapter 35. The Information Schema| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](server-programming.md "Part V. Server Programming")  
  
* * *

## 35.66. `views` #

The view `views` contains all views defined in the current database. Only those views are shown that the current user has access to (by way of being the owner or having some privilege). 

**Table 35.64.`views` Columns**

Column Type  Description   
---  
`table_catalog` `sql_identifier` Name of the database that contains the view (always the current database)   
`table_schema` `sql_identifier` Name of the schema that contains the view   
`table_name` `sql_identifier` Name of the view   
`view_definition` `character_data` Query expression defining the view (null if the view is not owned by a currently enabled role)   
`check_option` `character_data` `CASCADED` or `LOCAL` if the view has a `CHECK OPTION` defined on it, `NONE` if not   
`is_updatable` `yes_or_no` `YES` if the view is updatable (allows `UPDATE` and `DELETE`), `NO` if not   
`is_insertable_into` `yes_or_no` `YES` if the view is insertable into (allows `INSERT`), `NO` if not   
`is_trigger_updatable` `yes_or_no` `YES` if the view has an `INSTEAD OF` `UPDATE` trigger defined on it, `NO` if not   
`is_trigger_deletable` `yes_or_no` `YES` if the view has an `INSTEAD OF` `DELETE` trigger defined on it, `NO` if not   
`is_trigger_insertable_into` `yes_or_no` `YES` if the view has an `INSTEAD OF` `INSERT` trigger defined on it, `NO` if not   
  
  


* * *

[Prev](infoschema-view-table-usage.md "35.65. view_table_usage") | [Up](information-schema.md "Chapter 35. The Information Schema")|  [Next](server-programming.md "Part V. Server Programming")  
---|---|---  
35.65. `view_table_usage` | [Home](index.md "PostgreSQL 17.5 Documentation")|  Part V. Server Programming
