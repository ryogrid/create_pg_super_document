# getTSDictionaries

## Location
[src/bin/pg_dump/pg_dump.c:9460-9531](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L9460-L9531)

## Overview
This function reads all text search dictionaries from the PostgreSQL system catalogs and returns them in a TSDictInfo structure array for use by pg_dump.

## Definition
TSDictInfo *getTSDictionaries(Archive *fout, int *numTSDicts)

## Detailed Description
The getTSDictionaries function is part of the pg_dump utility that extracts metadata about text search dictionaries from the pg_ts_dict system catalog. It performs a comprehensive query to retrieve all text search dictionary objects and packages them into a structured format for dumping.

The function constructs a SQL query to select all relevant fields from pg_ts_dict, executes the query, and processes each result row to populate a TSDictInfo structure. Each dictionary object contains information about its name, namespace, owner, template, and initialization options. The function handles null initialization options appropriately and assigns dump IDs for dependency tracking.

## Parameters / Member Variables
- : Pointer to Archive structure representing the output destination for the dump
- : Pointer to integer that will be set to the total number of dictionaries retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md): Executes the SQL query against the database
  - pg_malloc: Allocates memory for the TSDictInfo array  
  - atooid: Converts string OID values to Oid type
  - [AssignDumpId](../A/AssignDumpId.md): Assigns unique dump ID to each dictionary object
  - [findNamespace](../f/findNamespace.md): Looks up namespace information for the dictionary
  - [getRoleName](getRoleName.md): Retrieves role name for the dictionary owner
  - [PQgetisnull](../P/PQgetisnull.md): Checks if a result field is null
  - [selectDumpableObject](../s/selectDumpableObject.md): Determines if the dictionary should be included in dump
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md): Main schema data collection function

## Notes and Other Information
- The function queries pg_ts_dict system catalog to retrieve dictionary metadata including name, namespace, owner, template, and initialization options
- Initialization options (dictinitoption) can be null and are handled with appropriate null checking
- Memory is allocated for the entire array of dictionaries at once using pg_malloc
- Each dictionary references a template via the dicttemplate OID field
- The TSDictInfo structure contains both dump object metadata and dictionary-specific configuration information