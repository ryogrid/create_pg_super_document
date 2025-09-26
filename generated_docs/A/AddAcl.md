# AddAcl

## Location
[src/bin/pg_dump/dumputils.c:655-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L655-L674)

## Overview
Appends a privilege keyword to a privilege list buffer, automatically inserting commas between entries as needed.

## Definition
static void AddAcl(PQExpBuffer aclbuf, const char *keyword, const char *subname)

## Detailed Description
This function is used to build comma-separated lists of privilege keywords when formatting ACL (Access Control List) entries. It automatically handles comma insertion between privilege entries and supports optional subnames for privileges that require additional specification (such as column-level privileges). The function checks if the buffer already contains content and adds a comma separator before appending the new privilege keyword.

## Parameters / Member Variables
- `aclbuf`: PQExpBuffer containing the accumulating privilege list
- `keyword`: The privilege keyword to append (e.g., "SELECT", "INSERT", etc.)
- `subname`: Optional subname for the privilege (e.g., column name for column privileges)

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)  
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
- Called from (representative examples):
  - CONVERT_PRIV macro (src/bin/pg_dump/dumputils.c:468, 473)

## Notes and Other Information
- This is a static function used internally within dumputils.c
- Primarily used by the CONVERT_PRIV macro for building privilege strings
- When subname is provided, it formats as "keyword(subname)"
- Essential for generating properly formatted ACL strings during PostgreSQL dump operations
- Handles the comma separation logic automatically to ensure valid privilege list syntax