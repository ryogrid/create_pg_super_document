# getTSTemplates

## Location
src/bin/pg_dump/pg_dump.c: 9532 - 9596

## Overview
This function reads all text search templates from the PostgreSQL system catalogs and returns them in a TSTemplateInfo structure array for use by pg_dump.

## Definition
TSTemplateInfo *getTSTemplates(Archive *fout, int *numTSTemplates)

## Detailed Description
The getTSTemplates function is part of the pg_dump utility that extracts metadata about text search templates from the pg_ts_template system catalog. Text search templates are used as blueprints for creating text search dictionaries, defining the initialization and lexize functions that dictionaries will use.

The function constructs a SQL query to select all relevant fields from pg_ts_template, executes the query, and processes each result row to populate a TSTemplateInfo structure. Each template object contains references to its initialization function (tmplinit) and lexize function (tmpllexize), which define the template's behavior when used to create dictionaries.

## Parameters / Member Variables
- : Pointer to Archive structure representing the output destination for the dump
- : Pointer to integer that will be set to the total number of templates retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md): Executes the SQL query against the database
  - pg_malloc: Allocates memory for the TSTemplateInfo array
  - atooid: Converts string OID values to Oid type
  - [AssignDumpId](../A/AssignDumpId.md): Assigns unique dump ID to each template object
  - [findNamespace](../f/findNamespace.md): Looks up namespace information for the template
  - [selectDumpableObject](../s/selectDumpableObject.md): Determines if the template should be included in dump
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md): Main schema data collection function

## Notes and Other Information
- The function queries pg_ts_template system catalog to retrieve template metadata including name, namespace, and function OIDs
- Templates define the fundamental behavior for text search dictionaries through tmplinit and tmpllexize function references
- The tmplinit function handles dictionary initialization, while tmpllexize handles the actual text processing
- Memory is allocated for the entire array of templates at once using pg_malloc
- The TSTemplateInfo structure contains both dump object metadata and template-specific function references
- Templates serve as the foundation for creating custom text search dictionaries with specific linguistic processing capabilities