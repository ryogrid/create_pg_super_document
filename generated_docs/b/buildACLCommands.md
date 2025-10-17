# buildACLCommands

## Location
[src/bin/pg_dump/dumputils.c:102-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L102-L363)

## Overview
The buildACLCommands function generates GRANT and REVOKE SQL commands for PostgreSQL database objects by comparing actual ACL (Access Control List) strings with base ACL permissions.

## Definition

```c
bool
buildACLCommands(const char *name, const char *subname, const char *nspname,
				 const char *type, const char *acls, const char *baseacls,
				 const char *owner, const char *prefix, int remoteVersion,
				 PQExpBuffer sql)
```
## Detailed Description
This function is the core ACL command builder in pg_dump utilities. It takes an object's current ACL string and compares it with the base (default) ACL to determine what GRANT and REVOKE commands need to be generated to recreate the object's permissions in a restored database.

The function performs differential analysis - it identifies privileges that need to be granted (present in actual ACL but not in base ACL) and privileges that need to be revoked (present in base ACL but not in actual ACL). This approach ensures that only the necessary permission changes are dumped, making the output more efficient and readable.

The function handles special cases including:
- Objects with default permissions (null ACLs)
- Owner grants appearing before public grants for consistency
- Grantor changes requiring SET SESSION AUTHORIZATION
- Privileges with and without GRANT OPTION
- Namespace-qualified object names

## Parameters / Member Variables
- `*name`: The object name, already quoted and formatted for use in commands
- `*subname`: The sub-object name if any (e.g., column name), already quoted; NULL if none
- `*nspname`: The namespace the object is in; NULL if none, not pre-quoted
- `*type`: The object type for GRANT command (TABLE, SEQUENCE, FUNCTION, etc.)
- `*acls`: The current ACL string fetched from the database
- `*baseacls`: The initial/default ACL string for this object type and owner
- `*owner`: Username of object owner (will be passed through fmtId); can be NULL
- `*prefix`: String to prefix to each generated command; typically empty or "ALTER DEFAULT PRIVILEGES "
- `remoteVersion`: Version of the source database
- `sql`: PQExpBuffer to append the generated SQL commands to
## Dependencies
- Functions called/Symbols referenced:
  - [parsePGArray](../p/parsePGArray.md) (for parsing ACL arrays)
  - [parseAclItem](../p/parseAclItem.md) (for parsing individual ACL entries)
  - [pg_malloc](../p/pg_malloc.md) (for memory allocation)
  - [fmtId](../f/fmtId.md) (for identifier quoting)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (for string formatting)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/destroyPQExpBuffer (for buffer management)

- Called from (representative examples):
  - [buildDefaultACLCommands](buildDefaultACLCommands.md) (in dumputils.c)
  - [dumpACL](../d/dumpACL.md) (in pg_dump.c)
  - [dumpRoleGUCPrivs](../d/dumpRoleGUCPrivs.md) (in pg_dumpall.c)
  - [dumpTablespaces](../d/dumpTablespaces.md) (in pg_dumpall.c)

## Notes and Other Information
- Returns true on success, false if ACL parsing fails
- The function preserves the order of privileges as they appear in the ACL string to maintain GRANT WITH GRANT OPTION dependencies
- Owner grants are always output before other grants for consistency
- When grantor differs from owner, SET SESSION AUTHORIZATION commands are generated
- The baseacls parameter can be either acldefault() result or pg_init_privs entry
- Special handling for default ACL processing where name can be empty string
- Location: src/bin/pg_dump/dumputils.c:102-363

## Simplified Source

```c
bool
buildACLCommands(const char *name, const char *subname, const char *nspname,
                 const char *type, const char *acls, const char *baseacls,
                 const char *owner, const char *prefix, int remoteVersion,
                 PQExpBuffer sql)
{
    char **aclitems = NULL, **baseitems = NULL;
    char **grantitems = NULL, **revokeitems = NULL;
    int naclitems = 0, nbaseitems = 0;
    int ngrantitems = 0, nrevokeitems = 0;
    PQExpBuffer grantee, grantor, privs, privswgo;
    PQExpBuffer firstsql, secondsql;

    // Handle NULL ACLs (default permissions)
    if (acls == NULL || *acls == '\0')
        return true;

    // Treat empty owner as NULL
    if (owner && *owner == '\0')
        owner = NULL;

    // Parse ACL arrays
    if (!parsePGArray(acls, &aclitems, &naclitems) ||
        !parsePGArray(baseacls, &baseitems, &nbaseitems)) {
        // Cleanup and return failure
        free(aclitems);
        free(baseitems);
        return false;
    }

    // Compare ACLs to find grants and revokes needed
    grantitems = (char **) pg_malloc(naclitems * sizeof(char *));
    for (int i = 0; i < naclitems; i++) {
        bool found = false;
        for (int j = 0; j < nbaseitems; j++) {
            if (strcmp(aclitems[i], baseitems[j]) == 0) {
                found = true;
                break;
            }
        }
        if (!found)
            grantitems[ngrantitems++] = aclitems[i];
    }

    revokeitems = (char **) pg_malloc(nbaseitems * sizeof(char *));
    for (int i = 0; i < nbaseitems; i++) {
        bool found = false;
        for (int j = 0; j < naclitems; j++) {
            if (strcmp(baseitems[i], aclitems[j]) == 0) {
                found = true;
                break;
            }
        }
        if (!found)
            revokeitems[nrevokeitems++] = baseitems[i];
    }

    // Initialize working buffers
    grantee = createPQExpBuffer();
    grantor = createPQExpBuffer();
    privs = createPQExpBuffer();
    privswgo = createPQExpBuffer();
    firstsql = createPQExpBuffer();
    secondsql = createPQExpBuffer();

    // Build REVOKE statements for removed privileges
    for (int i = 0; i < nrevokeitems; i++) {
        if (parseAclItem(revokeitems[i], type, name, subname, remoteVersion,
                         grantee, grantor, privs, NULL) && privs->len > 0) {
            appendPQExpBuffer(firstsql, "%sREVOKE %s ON %s ", prefix, privs->data, type);
            if (nspname && *nspname)
                appendPQExpBuffer(firstsql, "%s.", fmtId(nspname));
            if (name && *name)
                appendPQExpBuffer(firstsql, "%s ", name);
            appendPQExpBufferStr(firstsql, "FROM ");
            if (grantee->len == 0)
                appendPQExpBufferStr(firstsql, "PUBLIC;\n");
            else
                appendPQExpBuffer(firstsql, "%s;\n", fmtId(grantee->data));
        }
    }

    // Build GRANT statements for new privileges
    for (int i = 0; i < ngrantitems; i++) {
        if (parseAclItem(grantitems[i], type, name, subname, remoteVersion,
                         grantee, grantor, privs, privswgo)) {
            if (privs->len > 0 || privswgo->len > 0) {
                PQExpBuffer thissql;

                // Set default grantor to owner
                if (grantor->len == 0 && owner)
                    printfPQExpBuffer(grantor, "%s", owner);

                // Owner's grants go first
                if (owner && strcmp(grantee->data, owner) == 0 &&
                    strcmp(grantor->data, owner) == 0)
                    thissql = firstsql;
                else
                    thissql = secondsql;

                // Handle session authorization changes
                if (grantor->len > 0 && (!owner || strcmp(owner, grantor->data) != 0))
                    appendPQExpBuffer(thissql, "SET SESSION AUTHORIZATION %s;\n",
                                      fmtId(grantor->data));

                // Generate GRANT commands for regular privileges
                if (privs->len > 0) {
                    appendPQExpBuffer(thissql, "%sGRANT %s ON %s ", prefix, privs->data, type);
                    if (nspname && *nspname)
                        appendPQExpBuffer(thissql, "%s.", fmtId(nspname));
                    if (name && *name)
                        appendPQExpBuffer(thissql, "%s ", name);
                    appendPQExpBufferStr(thissql, "TO ");
                    if (grantee->len == 0)
                        appendPQExpBufferStr(thissql, "PUBLIC;\n");
                    else
                        appendPQExpBuffer(thissql, "%s;\n", fmtId(grantee->data));
                }

                // Generate GRANT commands for WITH GRANT OPTION privileges
                if (privswgo->len > 0) {
                    appendPQExpBuffer(thissql, "%sGRANT %s ON %s ", prefix, privswgo->data, type);
                    if (nspname && *nspname)
                        appendPQExpBuffer(thissql, "%s.", fmtId(nspname));
                    if (name && *name)
                        appendPQExpBuffer(thissql, "%s ", name);
                    appendPQExpBufferStr(thissql, "TO ");
                    if (grantee->len == 0)
                        appendPQExpBufferStr(thissql, "PUBLIC");
                    else
                        appendPQExpBufferStr(thissql, fmtId(grantee->data));
                    appendPQExpBufferStr(thissql, " WITH GRANT OPTION;\n");
                }

                // Reset session authorization if changed
                if (grantor->len > 0 && (!owner || strcmp(owner, grantor->data) != 0))
                    appendPQExpBufferStr(thissql, "RESET SESSION AUTHORIZATION;\n");
            }
        }
    }

    // Combine results and cleanup
    appendPQExpBuffer(sql, "%s%s", firstsql->data, secondsql->data);

    // Cleanup all buffers and arrays
    destroyPQExpBuffer(grantee);
    destroyPQExpBuffer(grantor);
    destroyPQExpBuffer(privs);
    destroyPQExpBuffer(privswgo);
    destroyPQExpBuffer(firstsql);
    destroyPQExpBuffer(secondsql);

    free(aclitems);
    free(baseitems);
    free(grantitems);
    free(revokeitems);

    return true;
}
```