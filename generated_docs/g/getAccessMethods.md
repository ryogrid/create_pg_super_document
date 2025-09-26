# getAccessMethods

## Location
[src/bin/pg_dump/pg_dump.c:6240-6319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6240-L6319)

## Overview
Reads all user-defined access methods from the PostgreSQL system catalogs and returns them in an AccessMethodInfo structure array for pg_dump processing.

## Definition
```c
AccessMethodInfo *getAccessMethods(Archive *fout, int *numAccessMethods)
```

## Detailed Description
The getAccessMethods function is part of pg_dump's catalog scanning infrastructure that retrieves all access method objects defined in the database. It queries the pg_am system catalog to collect access method metadata including names, handlers, and types. Access methods define how PostgreSQL stores and retrieves data (e.g., B-tree, Hash, GIN, GiST). The function handles version differences between PostgreSQL versions, with special handling for pre-9.6 systems that had a different access method API. For modern versions (9.6+), it retrieves the amhandler and amtype; for older versions, it provides default values. The function uses selectDumpableAccessMethod to determine which access methods should be included in the dump.

## Parameters / Member Variables
- `fout`: Archive structure containing connection and dump configuration information
- `numAccessMethods`: Output parameter that receives the total number of access methods found

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [selectDumpableAccessMethod](../s/selectDumpableAccessMethod.md)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Handles version compatibility between PostgreSQL 9.6+ (which introduced CREATE ACCESS METHOD) and earlier versions
- Access methods do not have namespaces, so the namespace field is set to NULL
- For pre-9.6 versions, the function facilitates OID-to-name mapping through findAccessMethodByOid
- The amhandler field contains the function that implements the access method's operations
- Each access method is assigned a dump ID for dependency tracking
- Memory allocation is done upfront for the entire access method array based on query results