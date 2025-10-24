# dumpDefaultACL

## Location
[src/bin/pg_dump/pg_dump.c:15173-15261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15173-L15261)

## Overview
Generates ALTER DEFAULT PRIVILEGES statements to recreate default access control lists for various database object types during database restoration.

## Definition

```c
static void
dumpDefaultACL(Archive *fout, const DefaultACLInfo *daclinfo)
```
## Detailed Description
The  function handles the dumping of default privileges (default ACLs) for database objects. Default privileges allow database administrators to set up access permissions that will be automatically applied to newly created objects of specific types within a schema or database.

The function performs the following key operations:
1. **Object type mapping** - Converts internal object type constants to human-readable strings (TABLES, SEQUENCES, FUNCTIONS, TYPES, SCHEMAS)
2. **Command generation** - Uses  to construct the appropriate ALTER DEFAULT PRIVILEGES statements
3. **Conditional dumping** - Respects dump options for data-only dumps and ACL skipping
4. **Archive integration** - Creates archive entries in the POST_DATA section to ensure proper restoration order

Default privileges are particularly important in multi-user environments where consistent permission schemes need to be maintained across object creation.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump configuration and output methods
- `*daclinfo`: DefaultACLInfo structure containing the default privileges information, including object type, role, namespace, ACL data, and dump flags
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [buildDefaultACLCommands](../b/buildDefaultACLCommands.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [pg_fatal](../p/pg_fatal.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - DEFACLOBJ_RELATION, DEFACLOBJ_SEQUENCE, DEFACLOBJ_FUNCTION, DEFACLOBJ_TYPE, DEFACLOBJ_NAMESPACE constants
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- The function includes safety checks and will terminate with a fatal error if an unrecognized object type is encountered
- Default privileges are dumped in the POST_DATA section to ensure they are applied after all objects have been created
- The function respects both  and  dump options, allowing users to exclude default privileges from dumps when needed
- Default privileges can be scoped to specific schemas or apply database-wide (when namespace is NULL)
- The actual SQL command construction is delegated to , which handles the complex logic of comparing current vs. default privileges

## Simplified Source

```c
static void
dumpDefaultACL(Archive *fout, const DefaultACLInfo *daclinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q, tag;
    const char *type;

    // Skip if data-only dump or ACLs are being skipped
    if (dopt->dataOnly || dopt->aclsSkip)
        return;

    // Initialize buffers
    q = createPQExpBuffer();
    tag = createPQExpBuffer();

    // Map object type to readable string
    switch (daclinfo->defaclobjtype) {
        case DEFACLOBJ_RELATION:
            type = "TABLES";
            break;
        case DEFACLOBJ_SEQUENCE:
            type = "SEQUENCES";
            break;
        case DEFACLOBJ_FUNCTION:
            type = "FUNCTIONS";
            break;
        case DEFACLOBJ_TYPE:
            type = "TYPES";
            break;
        case DEFACLOBJ_NAMESPACE:
            type = "SCHEMAS";
            break;
        default:
            pg_fatal("unrecognized object type in default privileges: %d",
                     (int) daclinfo->defaclobjtype);
            type = "";  // keep compiler quiet
    }

    // Create descriptive tag
    appendPQExpBuffer(tag, "DEFAULT PRIVILEGES FOR %s", type);

    // Build the ALTER DEFAULT PRIVILEGES commands
    if (!buildDefaultACLCommands(type,
                                 daclinfo->dobj.namespace != NULL ?
                                 daclinfo->dobj.namespace->dobj.name : NULL,
                                 daclinfo->dacl.acl,
                                 daclinfo->dacl.acldefault,
                                 daclinfo->defaclrole,
                                 fout->remoteVersion,
                                 q))
        pg_fatal("could not parse default ACL list (%s)", daclinfo->dacl.acl);

    // Create archive entry in POST_DATA section
    if (daclinfo->dobj.dump & DUMP_COMPONENT_ACL)
        ArchiveEntry(fout, daclinfo->dobj.catId, daclinfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = tag->data,
                                  .namespace = daclinfo->dobj.namespace ?
                                  daclinfo->dobj.namespace->dobj.name : NULL,
                                  .owner = daclinfo->defaclrole,
                                  .description = "DEFAULT ACL",
                                  .section = SECTION_POST_DATA,
                                  .createStmt = q->data));

    // Cleanup
    destroyPQExpBuffer(tag);
    destroyPQExpBuffer(q);
}
```