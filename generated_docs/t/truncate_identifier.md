# truncate_identifier

## Location
[src/backend/parser/scansup.c:93-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/scansup.c#L93-L116)

## Overview
A utility function that truncates SQL identifiers to PostgreSQL's maximum allowed length while properly handling multi-byte character boundaries and optionally issuing warnings.

## Definition

```c
void
truncate_identifier(char *ident, int len, bool warn)
```
## Detailed Description
This function ensures that identifiers do not exceed PostgreSQL's maximum identifier length (NAMEDATALEN-1 bytes) by truncating them in-place when necessary. The function is designed to handle multi-byte character encodings correctly by using  to find an appropriate truncation point that doesn't split multi-byte characters.

When truncation occurs, the function modifies the input string in-place by placing a null terminator at the appropriate position. If warnings are enabled, it issues a NOTICE-level message showing both the original identifier and the truncated result, helping users understand what happened to their identifier names.

The function is called as part of PostgreSQL's identifier normalization pipeline, typically after case conversion has been performed.

## Parameters / Member Variables
- `*ident`: Pointer to the identifier string to be truncated (modified in-place)
- `len`: Current length of the identifier string in bytes
- `warn`: Boolean flag indicating whether to emit a warning notice if truncation occurs
## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (maximum identifier length constant)
  - [pg_mbcliplen](../p/pg_mbcliplen.md) (multi-byte safe string clipping function)
  - ereport (error/notice reporting system)
  - NOTICE (message level constant)
  - [errcode](../e/errcode.md)/errmsg (error reporting macros)
  - ERRCODE_NAME_TOO_LONG (specific error code for name truncation)
- Called from (representative examples):
  - [base_yylex](../b/base_yylex.md) (lexical analyzer in parser)
  - [downcase_identifier](../d/downcase_identifier.md) (identifier case conversion)
  - [SplitIdentifierString](../S/SplitIdentifierString.md) (identifier parsing utilities)
  - [parse_and_validate_value](../p/parse_and_validate_value.md) (configuration parameter processing)

## Notes and Other Information
- Modifies the input string in-place rather than allocating new memory
- Uses  to ensure truncation respects multi-byte character boundaries
- The caller must pass the string length to avoid an extra  call for performance
- Issues a NOTICE (not an error) when truncation occurs, allowing processing to continue
- Critical for maintaining PostgreSQL's identifier length constraints across the system
- The truncation point is NAMEDATALEN-1 to leave room for the null terminator
- Part of PostgreSQL's identifier processing infrastructure in the parser subsystem

## Simplified Source

```c
void
truncate_identifier(char *ident, int len, bool warn)
{
    // Check if truncation is needed
    if (len >= NAMEDATALEN) {
        // Find safe truncation point for multi-byte characters
        len = pg_mbcliplen(ident, len, NAMEDATALEN - 1);

        // Issue warning if requested
        if (warn) {
            ereport(NOTICE,
                (errcode(ERRCODE_NAME_TOO_LONG),
                 errmsg("identifier \"%s\" will be truncated to \"%.*s\"",
                        ident, len, ident)));
        }

        // Terminate string at truncation point
        ident[len] = '\0';
    }
}
```