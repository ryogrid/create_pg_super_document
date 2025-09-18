# parseAclItem

## Location
src/bin/pg_dump/dumputils.c: 421 - 461

## Overview
The parseAclItem function parses individual ACL (Access Control List) item strings in PostgreSQL's "username=privilegecodes/grantor" format and converts privilege codes to human-readable GRANT/REVOKE commands.

## Definition


## Detailed Description
This function is a critical component of PostgreSQL's ACL parsing system that breaks down individual ACL entries into their constituent parts. Each ACL item has the format "username=privilegecodes/grantor" where privilege codes are single-character abbreviations (like 'r' for SELECT, 'w' for UPDATE, etc.).

The function performs several key operations:
1. Parses the grantee name (username or PUBLIC)
2. Extracts the grantor name  
3. Converts single-character privilege codes to full SQL privilege names
4. Separates privileges with grant option (marked with '*') from those without
5. Handles object-type-specific privilege mappings

The function supports all PostgreSQL object types including tables, sequences, functions, schemas, databases, tablespaces, types, foreign data wrappers, servers, parameters, and large objects. Each object type has its own set of valid privileges.

## Parameters / Member Variables
- : The ACL item string to parse (format: "username=privilegecodes/grantor")
- : The object type (TABLE, FUNCTION, SCHEMA, etc.) determining valid privileges
- : The object name (used for context, not directly parsed)
- : Sub-object name like column name (affects privilege applicability)
- : Version of source database (for compatibility)
- : Output buffer for the dequoted grantee username (empty for PUBLIC)
- : Output buffer for the dequoted grantor username
- : Output buffer for privileges without grant option
- : Output buffer for privileges with grant option (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](pg_strdup.md) (for string duplication)
  - [dequoteAclUserName](../d/dequoteAclUserName.md) (for parsing and dequoting usernames)
  - [pg_free](pg_free.md) (for memory deallocation)
  - strchr (for finding characters in strings)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)/printfPQExpBuffer/appendPQExpBuffer (for buffer management)
  - [AddAcl](../A/AddAcl.md) (macro for adding privileges to buffers)

- Called from (representative examples):
  - [buildACLCommands](../b/buildACLCommands.md) (twice, for parsing grant and revoke items)

## Notes and Other Information
- Returns true on successful parsing, false on parse errors
- Uses a CONVERT_PRIV macro internally to map privilege codes to SQL keywords
- Handles special case where all privileges are granted (outputs "ALL" instead of individual privileges)
- Privilege codes vary by object type (e.g., 'r'=SELECT for tables, 'X'=EXECUTE for functions)
- Grant option is indicated by '*' suffix after privilege code
- The function calls abort() for unknown object types, indicating a programming error
- Location: src/bin/pg_dump/dumputils.c:421-575
- This is a static function, only used within dumputils.c