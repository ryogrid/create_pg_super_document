# parse_ident_line

## Location
[src/backend/libpq/hba.c:2689-2756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L2689-L2756)

## Overview
Parses a single tokenized line from the pg_ident.conf file and creates an IdentLine structure containing the user mapping rule with compiled regular expressions.

## Definition

```c
enumber = line_num;
```
## Detailed Description
This function processes tokenized lines from the pg_ident.conf file, which defines mappings between system usernames and PostgreSQL role names. The function performs comprehensive validation and processing:

1. **Field Extraction**: Extracts the three required fields from the tokenized line (map name, system username pattern, PostgreSQL username pattern)
2. **Validation**: Ensures all required fields are present and contain single values (no multiple values allowed per field)
3. **Data Structure Creation**: Creates an IdentLine structure and populates it with the extracted information
4. **Regular Expression Compilation**: Compiles regular expressions for username patterns that begin with '/' (regex indicators)
5. **Memory Management**: Designed to work within a memory context that can be reset on error

The function supports both literal username matching and regular expression pattern matching. When a username pattern begins with a forward slash, it's treated as a regular expression and compiled for later use during authentication. This enables flexible user mapping scenarios such as domain-based mappings or pattern-based user transformations.

## Parameters / Member Variables
- : Pointer to TokenizedAuthLine containing the parsed tokens from pg_ident.conf
- : Error reporting level for ereport() calls
- Returns:  - parsed identity mapping rule, or NULL on error

## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md)/lnext (list manipulation)
  - lfirst/linitial (list element access)
  - [palloc0](palloc0.md) (memory allocation)
  - [pstrdup](pstrdup.md) (string duplication)
  - [copy_auth_token](../c/copy_auth_token.md) (authentication token duplication)
  - [regcomp_auth_token](../r/regcomp_auth_token.md) (regular expression compilation for auth tokens)
  - IDENT_MULTI_VALUE/IDENT_FIELD_ABSENT (validation macros)
  - [TokenizedAuthLine](../T/TokenizedAuthLine.md), AuthToken, IdentLine (data structures)
- Called from:
  - [load_ident](../l/load_ident.md) (src/backend/libpq/hba.c:2997)
  - [fill_ident_view](../f/fill_ident_view.md) (src/backend/utils/adt/hbafuncs.c:552)

## Notes and Other Information
- Memory leaks are acceptable on error since caller is expected to reset the memory context
- Regular expression patterns must begin with '/' to be recognized as regex
- The pg_ident.conf file format requires exactly three fields: mapname, system_username, pg_username
- Compiled regular expressions are stored in the IdentLine structure for efficient matching during authentication
- Error messages are stored in tok_line->err_msg for detailed reporting
- Part of PostgreSQL's identity mapping system used with ident, peer, GSSAPI, SSPI, and cert authentication methods