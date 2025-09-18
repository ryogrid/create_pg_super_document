# getTSParsers

## Location
src/bin/pg_dump/pg_dump.c: 9380 - 9459

## Overview
This function reads all text search parsers from the PostgreSQL system catalogs and returns them in a TSParserInfo structure array for use by pg_dump.

## Definition
TSParserInfo *getTSParsers(Archive *fout, int *numTSParsers)

## Detailed Description
The getTSParsers function is part of the pg_dump utility that extracts metadata about text search parsers from the pg_ts_parser system catalog. It performs a comprehensive query to retrieve all text search parser objects, including built-in ones, and packages them into a structured format for dumping.

The function constructs a SQL query to select all relevant fields from pg_ts_parser, executes the query, and then processes each result row to populate a TSParserInfo structure. Each parser object is assigned a dump ID and evaluated to determine if it should be dumped based on dump options (system-defined objects are typically filtered out at dump-out time).

## Parameters / Member Variables
- : Pointer to Archive structure representing the output destination for the dump
- : Pointer to integer that will be set to the total number of parsers retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md): Executes the SQL query against the database
  - pg_malloc: Allocates memory for the TSParserInfo array
  - atooid: Converts string OID values to Oid type
  - [AssignDumpId](../A/AssignDumpId.md): Assigns unique dump ID to each parser object
  - [findNamespace](../f/findNamespace.md): Looks up namespace information for the parser
  - [selectDumpableObject](../s/selectDumpableObject.md): Determines if the parser should be included in dump
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md): Main schema data collection function

## Notes and Other Information
- The function queries pg_ts_parser system catalog to retrieve parser metadata including start, token, end, headline, and lextype function OIDs
- All text search objects are initially retrieved, with filtering of system-defined objects deferred to dump-out time
- Memory is allocated for the entire array of parsers at once using pg_malloc
- Each parser is processed to extract its component function OIDs and namespace information
- The TSParserInfo structure contains both dump object metadata and parser-specific function references