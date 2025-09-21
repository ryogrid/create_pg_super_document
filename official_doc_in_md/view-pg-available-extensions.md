52.2. `pg_available_extensions`  
---  
[Prev](views-overview.md "52.1. Overview") | [Up](views.md "Chapter 52. System Views")| Chapter 52. System Views| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](view-pg-available-extension-versions.md "52.3. pg_available_extension_versions")  
  
* * *

## 52.2. `pg_available_extensions` #

The `pg_available_extensions` view lists the extensions that are available for installation. See also the [`pg_extension`](catalog-pg-extension.md "51.22. pg_extension") catalog, which shows the extensions currently installed. 

**Table 52.2.`pg_available_extensions` Columns**

Column Type  Description   
---  
`name` `name` Extension name   
`default_version` `text` Name of default version, or `NULL` if none is specified   
`installed_version` `text` Currently installed version of the extension, or `NULL` if not installed   
`comment` `text` Comment string from the extension's control file   
  
  


The `pg_available_extensions` view is read-only. 

* * *

[Prev](views-overview.md "52.1. Overview") | [Up](views.md "Chapter 52. System Views")|  [Next](view-pg-available-extension-versions.md "52.3. pg_available_extension_versions")  
---|---|---  
52.1. Overview | [Home](index.md "PostgreSQL 17.5 Documentation")|  52.3. `pg_available_extension_versions`
