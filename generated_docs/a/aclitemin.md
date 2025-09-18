# aclitemin

## Location
src/backend/utils/adt/acl.c: 615 - 645

## Overview
Parses a string representation of an ACL specification and creates a new AclItem structure from it.

## Definition
```c
Datum aclitemin(PG_FUNCTION_ARGS)
```

## Detailed Description
The aclitemin function is a PostgreSQL input function that converts a text string containing an ACL specification into an internal AclItem structure. It allocates memory for a new AclItem, uses the aclparse function to parse the input string, and validates that the entire string is consumed during parsing (no trailing garbage). The function follows PostgreSQL's standard input function conventions, accepting a C string as input and returning a Datum containing the parsed AclItem. It handles parsing errors gracefully using the error context mechanism.

## Parameters / Member Variables
- Input via PG_FUNCTION_ARGS:
  - `s`: C string containing the ACL specification to parse
  - `escontext`: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro to extract C string argument)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [aclparse](aclparse.md) (function to parse ACL string into AclItem)
  - isspace (standard library character classification)
  - ereturn (PostgreSQL error return with context)
  - PG_RETURN_ACLITEM_P (macro to return AclItem as Datum)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system)

## Notes and Other Information
- This is a PostgreSQL input function for the aclitem data type
- Allocates memory using palloc which is automatically freed at end of transaction
- Validates that the entire input string is consumed during parsing
- Returns NULL on parsing failure when error context allows soft errors
- Raises ERRCODE_INVALID_TEXT_REPRESENTATION for trailing garbage in input
- Part of PostgreSQL's type system infrastructure for ACL item input/output