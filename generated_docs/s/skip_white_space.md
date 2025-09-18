# skip_white_space

## Location
[src/bin/psql/common.c:1833-1896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L1833-L1896)

## Overview
skip_white_space advances a character pointer over whitespace and SQL comments, supporting both single-line (--) and multi-line (/* */) comment styles with proper nesting.

## Definition
static const char *skip_white_space(const char *query)

## Detailed Description
skip_white_space is a utility function that parses SQL text to skip over non-significant whitespace and comments, returning a pointer to the first meaningful SQL token. The function implements PostgreSQL's comment parsing rules with sophisticated handling of:

**Comment Types Supported:**
1. **Single-line comments (---)**: Skips from -- to end of line, ignoring any /* sequences within
2. **Multi-line comments (/* */)**: Supports proper nesting of slash-star comments
3. **Whitespace**: Skips all characters where isspace() returns true

**Multibyte Character Support:**
- Uses PQmblenBounded() to correctly handle multibyte character encodings
- Assumes encoding is ASCII-superset but doesn't assume multibyte character content
- Critical for proper parsing in international character sets
- Advances by character boundaries, not individual bytes

**Nesting Logic:**
- Maintains cnestlevel counter for nested /* */ comments
- Only processes -- comments when not inside /* */ comments
- Prevents /* sequences inside -- comments from starting new comment blocks

The function is essential for SQL parsing operations where comments and whitespace need to be ignored to find actual SQL commands.

## Parameters / Member Variables
- `query`: Pointer to the SQL string to parse, starting from current position

## Dependencies
- Functions called/Symbols referenced:
  - [PQmblenBounded](../P/PQmblenBounded.md) (for multibyte character length calculation)
  - isspace (standard C library function)
- Called from (representative examples):
  - [command_no_begin](../c/command_no_begin.md) (multiple calls for SQL parsing)

## Notes and Other Information
- Returns pointer to first non-whitespace, non-comment character in the query
- Function is static, only accessible within common.c
- Handles nested /* */ comments correctly (PostgreSQL extension)
- Single-line comments (--) take precedence over /* */ when not nested
- Critical for proper SQL command identification in psql
- Encoding-aware: uses pset.encoding for character boundary detection
- Used extensively by command_no_begin() for determining transaction behavior
- Preserves original string (read-only operation)