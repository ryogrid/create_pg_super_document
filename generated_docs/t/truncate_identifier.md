# truncate_identifier

## Location
src/backend/parser/scansup.c: 93 - 116

## Overview
A utility function that truncates SQL identifiers to PostgreSQL's maximum allowed length while properly handling multi-byte character boundaries and optionally issuing warnings.

## Definition


## Detailed Description
This function ensures that identifiers do not exceed PostgreSQL's maximum identifier length (NAMEDATALEN-1 bytes) by truncating them in-place when necessary. The function is designed to handle multi-byte character encodings correctly by using  to find an appropriate truncation point that doesn't split multi-byte characters.

When truncation occurs, the function modifies the input string in-place by placing a null terminator at the appropriate position. If warnings are enabled, it issues a NOTICE-level message showing both the original identifier and the truncated result, helping users understand what happened to their identifier names.

The function is called as part of PostgreSQL's identifier normalization pipeline, typically after case conversion has been performed.

## Parameters / Member Variables
- : Pointer to the identifier string to be truncated (modified in-place)
- : Current length of the identifier string in bytes
- : Boolean flag indicating whether to emit a warning notice if truncation occurs

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (maximum identifier length constant)
  - pg_mbcliplen (multi-byte safe string clipping function)
  - ereport (error/notice reporting system)
  - NOTICE (message level constant)
  - errcode/errmsg (error reporting macros)
  - ERRCODE_NAME_TOO_LONG (specific error code for name truncation)
- Called from (representative examples):
  - base_yylex (lexical analyzer in parser)
  - downcase_identifier (identifier case conversion)
  - SplitIdentifierString (identifier parsing utilities)
  - parse_and_validate_value (configuration parameter processing)

## Notes and Other Information
- Modifies the input string in-place rather than allocating new memory
- Uses  to ensure truncation respects multi-byte character boundaries
- The caller must pass the string length to avoid an extra  call for performance
- Issues a NOTICE (not an error) when truncation occurs, allowing processing to continue
- Critical for maintaining PostgreSQL's identifier length constraints across the system
- The truncation point is NAMEDATALEN-1 to leave room for the null terminator
- Part of PostgreSQL's identifier processing infrastructure in the parser subsystem