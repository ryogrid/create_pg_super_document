# dumpTransform

## Location
src/bin/pg_dump/pg_dump.c: 12833 - 12961

## Overview
Generates SQL DDL statements to recreate PostgreSQL transforms, which define how data types are converted between SQL and procedural language representations.

## Definition
```c
static void dumpTransform(Archive *fout, const TransformInfo *transform)
```

## Detailed Description
This function creates SQL CREATE TRANSFORM statements to recreate type transforms in PostgreSQL. Transforms define conversion functions between SQL data types and their representations in procedural languages (like PL/Python, PL/Perl, etc.). The function handles both directions of transformation: FROM SQL (converting SQL values to language-specific representations) and TO SQL (converting language values back to SQL types). Each transform can specify one or both conversion functions.

Key responsibilities include:
- Building CREATE TRANSFORM statements with appropriate function specifications
- Handling bidirectional transformations (FROM SQL and TO SQL functions)
- Resolving and validating transform function definitions
- Generating corresponding DROP TRANSFORM statements for clean replacements
- Managing transform comments and binary upgrade scenarios

## Parameters / Member Variables
- `fout`: Archive structure containing dump context and output formatting information
- `transform`: TransformInfo structure containing transform metadata including type OID, language OID, and function OIDs for both conversion directions

## Dependencies
- Functions called/Symbols referenced:
  - [findFuncByOid](../f/findFuncByOid.md)
  - [get_language_name](../g/get_language_name.md)
  - [getFormattedTypeName](../g/getFormattedTypeName.md)
  - [format_function_signature](../f/format_function_signature.md)
  - [fmtId](../f/fmtId.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - pg_log_warning
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- The function is skipped entirely during data-only dumps (when dopt->dataOnly is true)
- Transforms require at least one function (FROM SQL or TO SQL) to be valid
- Both transform functions are validated for existence before generating SQL
- Function names in transform definitions are always namespace-qualified for clarity
- Error handling includes warnings for missing function definitions or invalid transform configurations
- Transform dependencies are properly tracked and included in the archive entry
- Memory management uses PostgreSQL's PQExpBuffer system with proper cleanup
- Binary upgrade scenarios are supported through extension member handling
- The function handles the comma separation logic when both FROM SQL and TO SQL functions are present