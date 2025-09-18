# dumpCast

## Location
[src/bin/pg_dump/pg_dump.c:12728-12832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12728-L12832)

## Overview
Generates SQL DDL statements to recreate a PostgreSQL user-defined cast, handling different cast methods and contexts during database dump operations.

## Definition
```c
static void dumpCast(Archive *fout, const CastInfo *cast)
```

## Detailed Description
This function creates SQL CREATE CAST statements to recreate user-defined type casts in PostgreSQL. It handles three different cast methods: binary casts (direct bit-wise conversion), input/output casts (using type I/O functions), and function-based casts (using explicit conversion functions). The function also manages cast contexts (implicit, assignment, or explicit) and ensures proper qualification of function names when generating SQL output.

Key responsibilities include:
- Building CREATE CAST statements with appropriate method specifications
- Handling different cast contexts (AS ASSIGNMENT, AS IMPLICIT, or explicit)
- Resolving and formatting cast function signatures when applicable
- Generating corresponding DROP CAST statements for clean replacements
- Managing cast comments and binary upgrade scenarios

## Parameters / Member Variables
- `fout`: Archive structure containing dump context and output formatting information
- `cast`: CastInfo structure containing cast metadata including source/target types, cast method, context, and associated function OID

## Dependencies
- Functions called/Symbols referenced:
  - [findFuncByOid](../f/findFuncByOid.md)
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
- Cast methods are distinguished by COERCION_METHOD constants (BINARY, INOUT, FUNCTION)
- Function-based casts require validation that the cast function exists and is accessible
- Cast contexts are represented by single characters: 'a' for assignment, 'i' for implicit
- Function names in cast definitions are always namespace-qualified for clarity
- Error handling includes warnings for invalid cast method values or missing function definitions
- Memory management uses PostgreSQL's PQExpBuffer system with proper cleanup
- Binary upgrade scenarios are supported through extension member handling