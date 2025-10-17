# append_depends_on_extension

## Location
[src/bin/pg_dump/pg_dump.c:5300-5344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5300-L5344)

## Overview
Appends ALTER ... DEPENDS ON EXTENSION statements to a create query for objects that depend on PostgreSQL extensions.

## Definition
```c
static void append_depends_on_extension(Archive *fout,
                                       PQExpBuffer create,
                                       const DumpableObject *dobj,
                                       const char *catalog,
                                       const char *keyword,
                                       const char *objname)
```

## Detailed Description
This function is part of the pg_dump utility and handles the generation of ALTER ... DEPENDS ON EXTENSION statements for database objects that have dependencies on PostgreSQL extensions. When pg_dump encounters an object that depends on an extension (indicated by the `depends_on_ext` flag), this function queries the system catalogs to find all extensions that the object depends on and appends the appropriate ALTER statements to the object's creation script.

The function performs a SQL query against `pg_depend` and `pg_extension` tables to find extension dependencies with dependency type 'x' (extension dependency), then generates ALTER statements for each dependency found.

## Parameters / Member Variables
- `fout`: Archive structure containing connection and dump state information
- `create`: PQExpBuffer where the ALTER DEPENDS ON EXTENSION statements will be appended
- `dobj`: DumpableObject containing the object's metadata, including its OID and dependency flags
- `catalog`: Name of the system catalog containing the object (e.g., "pg_class", "pg_proc")
- `keyword`: SQL keyword for the object type in ALTER statements (e.g., "TABLE", "FUNCTION")
- `objname`: Name of the object as it should appear in the ALTER statement

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBuffer](appendPQExpBuffer.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [fmtId](../f/fmtId.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
  - [pg_free](../p/pg_free.md)
  - PGRES_TUPLES_OK
- Called from (representative examples):
  - [dumpFunc](../d/dumpFunc.md) (src/bin/pg_dump/pg_dump.c:12673)
  - [dumpTableSchema](../d/dumpTableSchema.md) (src/bin/pg_dump/pg_dump.c:16322)
  - [dumpIndex](../d/dumpIndex.md) (src/bin/pg_dump/pg_dump.c:17056)
  - [dumpConstraint](../d/dumpConstraint.md) (src/bin/pg_dump/pg_dump.c:17375)
  - [dumpTrigger](../d/dumpTrigger.md) (src/bin/pg_dump/pg_dump.c:17922)

## Notes and Other Information
- The function only operates when `dobj->depends_on_ext` is true, indicating the object has extension dependencies
- Uses a SQL query to find dependencies with deptype='x' (extension dependency type)
- The generated ALTER statements ensure that when the dump is restored, the object will be properly marked as depending on the required extensions
- Uses `fmtId()` for proper SQL identifier formatting and handles potential non-reentrancy issues by duplicating the object name
- This mechanism is crucial for maintaining extension dependency relationships during database dumps and restores

## Simplified Source

```c
static void append_depends_on_extension(Archive *fout,
                                        PQExpBuffer create,
                                        const DumpableObject *dobj,
                                        const char *catalog,
                                        const char *keyword,
                                        const char *objname) {
    if (dobj->depends_on_ext) {
        char *nm;
        PGresult *res;
        PQExpBuffer query;

        // Avoid fmtId() non-reentrancy by duplicating object name
        nm = pg_strdup(objname);

        query = createPQExpBuffer();

        // Query to find all extensions this object depends on
        appendPQExpBuffer(query,
                          "SELECT e.extname "
                          "FROM pg_catalog.pg_depend d, pg_catalog.pg_extension e "
                          "WHERE d.refobjid = e.oid AND classid = '%s'::pg_catalog.regclass "
                          "AND objid = '%u'::pg_catalog.oid AND deptype = 'x' "
                          "AND refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass",
                          catalog,
                          dobj->catId.oid);

        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        int ntups = PQntuples(res);
        int i_extname = PQfnumber(res, "extname");

        // Generate ALTER ... DEPENDS ON EXTENSION statement for each dependency
        for (int i = 0; i < ntups; i++) {
            appendPQExpBuffer(create, "\nALTER %s %s DEPENDS ON EXTENSION %s;",
                              keyword, nm,
                              fmtId(PQgetvalue(res, i, i_extname)));
        }

        // Cleanup
        PQclear(res);
        destroyPQExpBuffer(query);
        pg_free(nm);
    }
}
```