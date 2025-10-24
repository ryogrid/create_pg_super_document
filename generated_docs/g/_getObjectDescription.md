# _getObjectDescription

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3664-3757](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3664-L3757)

## Overview
Extracts an object description for a TOC entry and appends it to a buffer, primarily used for generating ALTER ... OWNER TO statements in pg_dump operations.

## Definition

```c
static void
_getObjectDescription(PQExpBuffer buf, const TocEntry *te)
```
## Detailed Description
This function builds appropriate object descriptions for different PostgreSQL database objects based on their type. It handles three main categories of objects:

1. **Objects with simple decoration**: Tables, views, sequences, domains, etc. - formatted as "TYPE [schema.]name"
2. **Objects requiring complex decoration**: Aggregates, functions, operators, procedures - uses information from DROP statements
3. **Objects without owners**: Constraints, indexes, triggers, etc. - no description is generated

The function uses string comparisons to determine the object type from the TOC entry's description field and formats the output accordingly. For objects that require schema qualification, it includes the namespace prefix when available.

## Parameters / Member Variables
- `buf`: PQExpBuffer to append the object description to
- `*te`: Pointer to TocEntry structure containing object metadata including type (desc), name (tag), namespace, and drop statement
## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (struct type)
  - [fmtId](../f/fmtId.md) (for identifier quoting)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (for formatted string appending)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) (for string appending)
  - [pg_strdup](../p/pg_strdup.md) (for string duplication)
  - [pg_fatal](../p/pg_fatal.md) (for error reporting)
  - strcmp (for string comparison)
- Called from:
  - [_printTocEntry](../p/_printTocEntry.md) (main caller for generating owner change statements)

## Notes and Other Information
- Function is static and only used within pg_backup_archiver.c
- Handles a comprehensive list of PostgreSQL object types including newer additions like publications and subscriptions
- For complex objects like functions and operators, it cleverly reuses the DROP statement syntax by removing the "DROP " prefix
- Objects without owners (constraints, indexes, etc.) result in no output, which is correct behavior since they inherit ownership from their parent objects
- Large objects (BLOBs) receive special formatting as "LARGE OBJECT" followed by their numeric identifier

## Simplified Source

```c
static void _getObjectDescription(PQExpBuffer buf, const TocEntry *te) {
    const char *type = te->desc;

    // Objects with simple name formatting
    if (strcmp(type, "TABLE") == 0 || strcmp(type, "VIEW") == 0 ||
        strcmp(type, "SEQUENCE") == 0 || strcmp(type, "DOMAIN") == 0 ||
        strcmp(type, "TYPE") == 0 || strcmp(type, "SCHEMA") == 0 ||
        // ... (and other simple types)
        strcmp(type, "SUBSCRIPTION") == 0) {

        appendPQExpBuffer(buf, "%s ", type);
        if (te->namespace && *te->namespace) {
            appendPQExpBuffer(buf, "%s.", fmtId(te->namespace));
        }
        appendPQExpBufferStr(buf, fmtId(te->tag));
    }
    // Large objects have numeric names
    else if (strcmp(type, "BLOB") == 0) {
        appendPQExpBuffer(buf, "LARGE OBJECT %s", te->tag);
    }
    // Complex objects (functions, operators, etc.) - extract from DROP statement
    else if (strcmp(type, "FUNCTION") == 0 || strcmp(type, "PROCEDURE") == 0 ||
             strcmp(type, "AGGREGATE") == 0 || strcmp(type, "OPERATOR") == 0) {

        // Remove "DROP " prefix and clean up the statement
        char *first = pg_strdup(te->dropStmt + 5);
        char *last = first + strlen(first) - 1;

        // Strip trailing semicolons and newlines
        while (last >= first && (*last == '\n' || *last == ';')) {
            last--;
        }
        *(last + 1) = '\0';

        appendPQExpBufferStr(buf, first);
        free(first);
        return;
    }
    // Objects without owners (constraints, indexes, etc.) - do nothing
    else if (strcmp(type, "INDEX") == 0 || strcmp(type, "CONSTRAINT") == 0 ||
             strcmp(type, "TRIGGER") == 0 || strcmp(type, "RULE") == 0) {
        // No action needed - these objects don't have separate owners
    }
    else {
        pg_fatal("don't know how to set owner for object type \"%s\"", type);
    }
}
```