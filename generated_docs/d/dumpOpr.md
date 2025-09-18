# dumpOpr

## Location
src/bin/pg_dump/pg_dump.c: 12962 - 13180

## Overview
Generates SQL DDL statements to recreate PostgreSQL user-defined operators, including all operator properties and associated functions during database dump operations.

## Definition
```c
static void dumpOpr(Archive *fout, const OprInfo *oprinfo)
```

## Detailed Description
This function creates SQL CREATE OPERATOR statements to recreate user-defined operators in PostgreSQL. It handles all operator properties including the implementation function, left and right operand types, commutator and negator operators, restriction and join selectivity functions, and operator characteristics like merge and hash join support. The function uses prepared statements for efficiency and handles different operator kinds (binary, left unary, right unary), though postfix operators are deprecated in PostgreSQL 14+.

Key responsibilities include:
- Building CREATE OPERATOR statements with complete operator specifications
- Handling different operator kinds: binary ('b'), left unary ('l'), and right unary ('r')
- Processing operator relationships (commutator, negator)
- Managing selectivity estimation functions (restriction and join)
- Formatting operator signatures for proper identification
- Generating corresponding DROP OPERATOR statements for clean replacements
- Handling deprecated postfix operator warnings

## Parameters / Member Variables
- `fout`: Archive structure containing dump context and output formatting information  
- `oprinfo`: OprInfo structure containing operator metadata including OID, name, implementation function, operand types, and various operator properties

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [convertRegProcReference](../c/convertRegProcReference.md)
  - [getFormattedOperatorName](../g/getFormattedOperatorName.md)
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
- Invalid operators (those without valid oprcode) are silently skipped
- Uses prepared statements (PREPQUERY_DUMPOPR) for performance optimization when dumping multiple operators
- Postfix operators ('r' kind) generate warnings in PostgreSQL 14+ as they are no longer supported
- Operator signatures are formatted to include operand types, using "NONE" for missing operands in unary operators
- Function references are converted using convertRegProcReference for proper formatting
- Operator relationships (commutator, negator) are resolved and formatted using getFormattedOperatorName
- Memory management uses PostgreSQL's PQExpBuffer system with proper cleanup
- Binary upgrade scenarios are supported through extension member handling