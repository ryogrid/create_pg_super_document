# dumpFunc

## Location
src/bin/pg_dump/pg_dump.c: 12312 - 12727

## Overview
Generates SQL DDL statements to recreate a PostgreSQL function, including all its attributes, parameters, and metadata during a database dump operation.

## Definition
```c
static void dumpFunc(Archive *fout, const FuncInfo *finfo)
```

## Detailed Description
This function is responsible for dumping a complete SQL CREATE FUNCTION (or PROCEDURE) statement to recreate a PostgreSQL function. It handles all function attributes including volatility, strictness, security properties, cost settings, parallelism, language, transforms, and configuration parameters. The function uses prepared statements for efficiency when dumping multiple functions and adapts its behavior based on the PostgreSQL server version to ensure compatibility across different releases.

Key responsibilities include:
- Building comprehensive CREATE FUNCTION/PROCEDURE statements
- Handling different function types (regular functions, procedures, window functions)
- Managing function source code, binary paths, and SQL body formats
- Processing function configuration parameters and GUC settings
- Generating appropriate DROP statements for clean replacements
- Adding comments, security labels, and ACL information
- Supporting binary upgrade scenarios

## Parameters / Member Variables
- `fout`: Archive structure containing dump context, options, and output formatting information
- `finfo`: FuncInfo structure containing complete function metadata including OID, name, arguments, return type, and various function attributes

## Dependencies
- Functions called/Symbols referenced:
  - format_function_arguments
  - format_function_signature
  - ExecuteSqlStatement
  - ExecuteSqlQueryForSingleRow
  - appendStringLiteralAH
  - appendStringLiteralDQ
  - parsePGArray
  - parseOidArray
  - getFormattedTypeName
  - variable_is_guc_list_quote
  - SplitGUCList
  - append_depends_on_extension
  - binary_upgrade_extension_member
  - ArchiveEntry
  - dumpComment
  - dumpSecLabel
  - dumpACL
- Called from (representative examples):
  - dumpDumpableObject
  - fmtQualifiedDumpable

## Notes and Other Information
- The function is skipped entirely during data-only dumps (when dopt->dataOnly is true)
- Uses prepared statements (PREPQUERY_DUMPFUNC) for performance optimization when dumping multiple functions
- Adapts SQL generation based on PostgreSQL version (supports features from 9.5+ to 14.0+)
- Handles three different function source formats: SQL body (14.0+), binary + source, or source only
- Processes GUC configuration parameters with special handling for list-quote variables
- Supports both functions and procedures (introduced in PostgreSQL 11)
- Includes comprehensive error handling for invalid function attributes
- Memory management uses PostgreSQL's PQExpBuffer system with proper cleanup
- Function signatures are generated in multiple formats for different purposes (identity vs full signatures)