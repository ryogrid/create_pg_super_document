# dumpACL

## Location
[src/bin/pg_dump/pg_dump.c:15262-15389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15262-L15389)

## Overview
Generates GRANT and REVOKE statements necessary to recreate access control lists (ACLs) for database objects during database restoration.

## Definition

```c
static DumpId
dumpACL(Archive *fout, DumpId objDumpId, DumpId altDumpId,
		const char *type, const char *name, const char *subname,
		const char *nspname, const char *tag, const char *owner,
		const DumpableAcl *dacl)
```
## Detailed Description
The  function is a central component of pg_dump's ACL handling system. It generates the SQL statements needed to recreate the access control permissions for various database objects during database restoration. The function handles complex permission scenarios including:

1. **Permission state transitions** - Calculates the difference between default permissions and current permissions to generate minimal GRANT/REVOKE commands
2. **Binary upgrade support** - Special handling for extension objects that need to preserve initial privileges in pg_init_privs during binary upgrades
3. **Dependency management** - Creates proper dependency relationships between the ACL entry and the underlying object(s)
4. **Multiple object types** - Supports ACLs for tables, sequences, functions, languages, schemas, databases, tablespaces, foreign data wrappers, servers, and large objects

The function uses  to perform the complex logic of comparing ACL states and generating the appropriate SQL commands. It only creates archive entries when there are actual permission changes to apply.

## Parameters / Member Variables
- `*fout`: Archive structure for output generation and configuration
- `objDumpId`: Dump ID of the primary object whose ACL is being dumped
- `altDumpId`: Optional secondary dump ID for additional dependencies, or InvalidDumpId
- `*type`: Object type string (TABLE, SEQUENCE, FUNCTION, etc.)
- `*name`: Properly formatted and quoted object name
- `*subname`: Formatted sub-object name (typically for table columns), or NULL
- `*nspname`: Namespace/schema name, or NULL for global objects
- `*tag`: Custom tag for the ACL TOC entry, or NULL to use default
- `*owner`: Object owner name, or NULL for ownerless objects like languages
- `*dacl`: DumpableAcl structure containing current ACL, default ACL, privilege type, and initial privileges
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [buildACLCommands](../b/buildACLCommands.md)
  - [createDumpId](../c/createDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [pg_fatal](../p/pg_fatal.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - InvalidDumpId constant
- Called from (representative examples):
  - [dumpTable](dumpTable.md)
  - [dumpFunc](dumpFunc.md)
  - [dumpNamespace](dumpNamespace.md)
  - [dumpForeignServer](dumpForeignServer.md)
  - [dumpDatabase](dumpDatabase.md)

## Notes and Other Information
- Returns the dump ID of the created ACL entry, or InvalidDumpId if no ACL entry was needed
- Respects  option to allow dumps without ACL information
- In data-only dumps, only large object ACLs are included (all other ACLs are skipped)
- Binary upgrade mode includes special logic to preserve pg_init_privs contents for extension objects
- The function handles the case where a NULL/empty ACL string represents the object-type-specific default
- ACL entries are created in SECTION_NONE, allowing them to be applied at the appropriate time during restoration
- Proper dependency tracking ensures ACLs are applied after the underlying objects exist

## Simplified Source

```c
static DumpId
dumpACL(Archive *fout, DumpId objDumpId, DumpId altDumpId,
        const char *type, const char *name, const char *subname,
        const char *nspname, const char *tag, const char *owner,
        const DumpableAcl *dacl)
{
    DumpId aclDumpId = InvalidDumpId;
    DumpOptions *dopt = fout->dopt;
    const char *acls = dacl->acl;
    const char *acldefault = dacl->acldefault;
    char privtype = dacl->privtype;
    const char *initprivs = dacl->initprivs;
    const char *baseacls;
    PQExpBuffer sql;

    // Skip if ACLs are disabled or data-only mode (except large objects)
    if (dopt->aclsSkip)
        return InvalidDumpId;

    if (dopt->dataOnly && strcmp(type, "LARGE OBJECT") != 0)
        return InvalidDumpId;

    sql = createPQExpBuffer();

    // Handle binary upgrade: preserve initial privileges for extension objects
    if (dopt->binary_upgrade && privtype == 'e' &&
        initprivs && *initprivs != '\0') {
        appendPQExpBufferStr(sql, "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\n");

        if (!buildACLCommands(name, subname, nspname, type,
                              initprivs, acldefault, owner,
                              "", fout->remoteVersion, sql))
            pg_fatal("could not parse initial ACL list (%s) or default (%s) for object \"%s\" (%s)",
                     initprivs, acldefault, name, type);

        appendPQExpBufferStr(sql, "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\n");
    }

    // Determine base privileges for comparison
    if (initprivs && *initprivs != '\0') {
        baseacls = initprivs;
        // Handle NULL/empty ACL as default ACL
        if (acls == NULL || *acls == '\0')
            acls = acldefault;
    } else {
        baseacls = acldefault;
    }

    // Generate GRANT/REVOKE commands for current ACL
    if (!buildACLCommands(name, subname, nspname, type,
                          acls, baseacls, owner,
                          "", fout->remoteVersion, sql))
        pg_fatal("could not parse ACL list (%s) or default (%s) for object \"%s\" (%s)",
                 acls, baseacls, name, type);

    // Create archive entry if SQL commands were generated
    if (sql->len > 0) {
        PQExpBuffer tagbuf = createPQExpBuffer();
        DumpId aclDeps[2];
        int nDeps = 0;

        // Create appropriate tag for the ACL entry
        if (tag)
            appendPQExpBufferStr(tagbuf, tag);
        else if (subname)
            appendPQExpBuffer(tagbuf, "COLUMN %s.%s", name, subname);
        else
            appendPQExpBuffer(tagbuf, "%s %s", type, name);

        // Set up dependencies
        aclDeps[nDeps++] = objDumpId;
        if (altDumpId != InvalidDumpId)
            aclDeps[nDeps++] = altDumpId;

        aclDumpId = createDumpId();

        // Create archive entry for the ACL
        ArchiveEntry(fout, nilCatalogId, aclDumpId,
                     ARCHIVE_OPTS(.tag = tagbuf->data,
                                  .namespace = nspname,
                                  .owner = owner,
                                  .description = "ACL",
                                  .section = SECTION_NONE,
                                  .createStmt = sql->data,
                                  .deps = aclDeps,
                                  .nDeps = nDeps));

        destroyPQExpBuffer(tagbuf);
    }

    destroyPQExpBuffer(sql);
    return aclDumpId;
}
```