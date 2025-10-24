# createDummyViewAsClause

## Location
[src/bin/pg_dump/pg_dump.c:15906-15945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15906-L15945)

## Overview
Creates a dummy AS clause for a PostgreSQL view definition used when the real view definition must be postponed due to circular dependencies between database objects.

## Definition

```c
static PQExpBuffer
createDummyViewAsClause(Archive *fout, const TableInfo *tbinfo)
```
## Detailed Description
This function generates a placeholder SELECT statement for a view that maintains the view's external properties (column names, types, and collations) while using NULL values for all columns. This is essential in pg_dump when circular dependencies prevent the immediate creation of a view with its actual definition. The dummy view preserves the schema structure so that dependent objects can reference it correctly, and it can later be replaced with the real view definition using CREATE OR REPLACE VIEW.

The function constructs a SELECT statement where each column is represented as "NULL::type_name COLLATE collation AS column_name", ensuring that the view interface remains consistent for dependent objects.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump context and configuration
- `*tbinfo`: TableInfo structure containing the view's metadata including column names, types, and collations
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)  
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - OidIsValid
  - [findCollationByOid](../f/findCollationByOid.md)
  - fmtQualifiedDumpable
  - [fmtId](../f/fmtId.md)
- Types referenced:
  - [Archive](../A/Archive.md)
  - [TableInfo](../T/TableInfo.md)
  - [CollInfo](../C/CollInfo.md)
  - PQExpBuffer
- Called from:
  - [dumpTableSchema](../d/dumpTableSchema.md)
  - [dumpRule](../d/dumpRule.md)

## Notes and Other Information
- Returns a newly allocated PQExpBuffer that must be freed by the caller
- Handles collation specifications to ensure CREATE OR REPLACE VIEW operations preserve collations
- Only adds collation clauses for non-default collations to avoid redundancy
- Essential for resolving circular dependency issues in complex database schemas during pg_dump operations

## Simplified Source

```c
static PQExpBuffer
createDummyViewAsClause(Archive *fout, const TableInfo *tbinfo)
{
    PQExpBuffer result = createPQExpBuffer();

    appendPQExpBufferStr(result, "SELECT");

    // Create NULL placeholder for each column
    for (int j = 0; j < tbinfo->numatts; j++)
    {
        if (j > 0)
            appendPQExpBufferChar(result, ',');
        appendPQExpBufferStr(result, "\n    ");

        // Add typed NULL value
        appendPQExpBuffer(result, "NULL::%s", tbinfo->atttypnames[j]);

        // Add collation if non-default
        if (OidIsValid(tbinfo->attcollation[j]))
        {
            CollInfo *coll = findCollationByOid(tbinfo->attcollation[j]);
            if (coll)
                appendPQExpBuffer(result, " COLLATE %s",
                                fmtQualifiedDumpable(coll));
        }

        // Add column alias
        appendPQExpBuffer(result, " AS %s", fmtId(tbinfo->attnames[j]));
    }

    return result;
}
```