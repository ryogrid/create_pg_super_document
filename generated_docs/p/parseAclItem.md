# parseAclItem

## Location
[src/bin/pg_dump/dumputils.c:421-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L421-L461)

## Overview
The parseAclItem function parses individual ACL (Access Control List) item strings in PostgreSQL's "username=privilegecodes/grantor" format and converts privilege codes to human-readable GRANT/REVOKE commands.

## Definition

```c
static bool
parseAclItem(const char *item, const char *type,
			 const char *name, const char *subname, int remoteVersion,
			 PQExpBuffer grantee, PQExpBuffer grantor,
			 PQExpBuffer privs, PQExpBuffer privswgo)
```
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
- `*item`: The ACL item string to parse (format: "username=privilegecodes/grantor")
- `*type`: The object type (TABLE, FUNCTION, SCHEMA, etc.) determining valid privileges
- `*name`: The object name (used for context, not directly parsed)
- `*subname`: Sub-object name like column name (affects privilege applicability)
- `remoteVersion`: Version of source database (for compatibility)
- `grantee`: Output buffer for the dequoted grantee username (empty for PUBLIC)
- `grantor`: Output buffer for the dequoted grantor username
- `privs`: Output buffer for privileges without grant option
- `privswgo`: Output buffer for privileges with grant option (can be NULL)
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

## Simplified Source

```c
static bool
parseAclItem(const char *item, const char *type,
             const char *name, const char *subname, int remoteVersion,
             PQExpBuffer grantee, PQExpBuffer grantor,
             PQExpBuffer privs, PQExpBuffer privswgo)
{
    char *buf = pg_strdup(item);
    char *eqpos, *slpos, *pos;
    bool all_with_go = true, all_without_go = true;

    // Parse grantee name (before '=')
    eqpos = dequoteAclUserName(grantee, buf);
    if (*eqpos != '=') {
        pg_free(buf);
        return false;
    }

    // Parse grantor name (after '/')
    slpos = strchr(eqpos + 1, '/');
    if (!slpos) {
        pg_free(buf);
        return false;
    }
    *slpos++ = '\0';
    slpos = dequoteAclUserName(grantor, slpos);
    if (*slpos != '\0') {
        pg_free(buf);
        return false;
    }

    // Reset output buffers
    resetPQExpBuffer(privs);
    if (privswgo) resetPQExpBuffer(privswgo);

    // Parse privilege codes between '=' and '/'
    for (pos = eqpos + 1; *pos && *pos != '/'; pos++) {
        char priv_code = *pos;
        bool has_grant_option = (pos[1] == '*');

        if (has_grant_option) pos++; // Skip '*'

        // Convert privilege code to SQL privilege name based on object type
        const char *priv_name = convert_privilege_code(priv_code, type);
        if (!priv_name) continue; // Skip unknown privileges

        // Add to appropriate privilege list
        if (has_grant_option && privswgo) {
            AddAcl(privswgo, priv_name, NULL);
            all_without_go = false;
        } else {
            AddAcl(privs, priv_name, NULL);
            all_with_go = false;
        }
    }

    // Optimize output: use "ALL" if all standard privileges are present
    if (all_with_go && privswgo && privswgo->len > 0) {
        resetPQExpBuffer(privswgo);
        printfPQExpBuffer(privswgo, "ALL");
    }
    if (all_without_go && privs->len > 0) {
        resetPQExpBuffer(privs);
        printfPQExpBuffer(privs, "ALL");
    }

    pg_free(buf);
    return true;
}
```