# binary_upgrade_extension_member

## Location
[src/bin/pg_dump/pg_dump.c:5589-5635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5589-L5635)

## Overview
Generates ALTER EXTENSION ADD commands during binary upgrades to restore extension membership relationships for database objects.

## Definition

```c
static void
binary_upgrade_extension_member(PQExpBuffer upgrade_buffer,
								const DumpableObject *dobj,
								const char *objtype,
								const char *objname,
								const char *objnamespace)
```
## Detailed Description
This function is part of PostgreSQL's pg_dump utility and handles extension membership during binary upgrades. When an object is a member of an extension, this function adds the appropriate ALTER EXTENSION ADD command to the upgrade buffer. This ensures that after a binary upgrade, objects maintain their correct extension membership relationships.

The function searches through the object's dependencies to find its parent extension, then generates an ALTER EXTENSION ADD command with the proper object type and qualified name. This is necessary because during binary upgrades, extension membership information needs to be explicitly restored.

## Parameters / Member Variables
- : PQExpBuffer to append the ALTER EXTENSION ADD command to
- : The DumpableObject that may be an extension member
- : String describing the type of object (e.g., "FUNCTION", "TABLE")
- : The object name, already quoted for SQL usage
- : The namespace/schema name (not quoted), can be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByDumpId](../f/findObjectByDumpId.md)
  - [fmtId](../f/fmtId.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
- Called from (representative examples):
  - [dumpNamespace](../d/dumpNamespace.md)
  - [dumpFunc](../d/dumpFunc.md)
  - [dumpTableSchema](../d/dumpTableSchema.md)
  - [dumpSequence](../d/dumpSequence.md)
  - [dumpCollation](../d/dumpCollation.md)

## Notes and Other Information
- Only processes objects that are extension members (dobj->ext_member must be true)
- Assumes member objects have a direct dependency on their parent extension
- The objname parameter should already be quoted, while objnamespace should not be quoted
- Generates SQL comments explaining the purpose for binary upgrade handling
- Used extensively throughout pg_dump for various object types to maintain extension relationships