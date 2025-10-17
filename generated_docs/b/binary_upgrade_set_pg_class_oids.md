# binary_upgrade_set_pg_class_oids

## Location
[src/bin/pg_dump/pg_dump.c:5473-5588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5473-L5588)

## Overview
Generates binary upgrade commands to preserve pg_class OIDs and relfilenodes for relations and their associated TOAST tables and indexes during PostgreSQL binary upgrades.

## Definition
```c
static void binary_upgrade_set_pg_class_oids(Archive *fout,
                                             PQExpBuffer upgrade_buffer, 
                                             Oid pg_class_oid,
                                             bool is_index)
```

## Detailed Description
This function is a critical component of PostgreSQL's binary upgrade infrastructure that ensures the preservation of physical storage identifiers (OIDs and relfilenodes) across major version upgrades. It handles the complex task of preserving not only the main relation's identifiers but also those of its associated TOAST table and TOAST index when they exist.

The function queries the system catalogs to retrieve the current OIDs and relfilenodes for a relation and its associated storage structures, then generates appropriate SQL statements calling binary upgrade helper functions. It handles different relation types (tables vs indexes) and includes special logic for TOAST tables, which store large attribute values separately from the main table.

A key complexity addressed is that older databases might have TOAST tables that are no longer needed due to schema changes (e.g., wide columns being dropped), but the OIDs must still be preserved to ensure successful file copying during the upgrade process.

## Parameters / Member Variables
- `fout`: Archive structure containing database connection and version information
- `upgrade_buffer`: PQExpBuffer where the generated binary upgrade SQL statements are appended
- `pg_class_oid`: The OID of the primary relation (table or index) being processed
- `is_index`: Boolean flag indicating whether the relation is an index (true) or a table/other relation (false)

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/destroyPQExpBuffer (buffer management)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)/appendPQExpBufferStr/appendPQExpBufferChar (SQL construction)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md) (database query execution)
  - atooid (string to OID conversion)
  - [PQgetvalue](../P/PQgetvalue.md)/PQfnumber/PQclear (query result handling)
  - RelFileNumberIsValid (relfilenode validation)
  - OidIsValid (OID validation)
  - [RelFileNumber](../R/RelFileNumber.md) (type for relation file numbers)
  - RELKIND_PARTITIONED_TABLE (constant for partitioned table relation kind)
- Called from (representative examples):
  - [dumpTableSchema](../d/dumpTableSchema.md) (src/bin/pg_dump/pg_dump.c:15989, 16091)
  - [dumpIndex](../d/dumpIndex.md) (src/bin/pg_dump/pg_dump.c:17003)
  - [dumpConstraint](../d/dumpConstraint.md) (src/bin/pg_dump/pg_dump.c:17271)
  - [dumpSequence](../d/dumpSequence.md) (src/bin/pg_dump/pg_dump.c:17690)
  - [dumpCompositeType](../d/dumpCompositeType.md) (src/bin/pg_dump/pg_dump.c:11855)

## Notes and Other Information
- Essential for maintaining storage file identity during binary upgrades, enabling direct file copying
- Handles version-specific behaviors: partitioned tables in pre-v12 databases had relfilenodes that should not be preserved
- Uses complex JOIN query to gather information about main relation, TOAST table, and TOAST index in single operation
- TOAST table preservation ensures that binary upgrade works even when TOAST tables exist in old database but aren't needed in new schema
- Different code paths for regular relations vs indexes, with indexes requiring simpler handling
- Generated SQL calls specialized binary upgrade functions like `binary_upgrade_set_next_heap_pg_class_oid` and `binary_upgrade_set_next_index_pg_class_oid`
- Critical for maintaining referential integrity and avoiding OID conflicts during major version upgrades

## Simplified Source

```c
static void binary_upgrade_set_pg_class_oids(Archive *fout,
                                             PQExpBuffer upgrade_buffer,
                                             Oid pg_class_oid,
                                             bool is_index)
{
    PQExpBuffer query = createPQExpBuffer();
    PGresult *res;
    RelFileNumber relfilenumber, toast_relfilenumber, toast_index_relfilenumber;
    Oid toast_oid, toast_index_oid;
    char relkind;

    // Query to get relation info including TOAST table and index details
    appendPQExpBuffer(query,
                      "SELECT c.relkind, c.relfilenode, c.reltoastrelid, "
                      "ct.relfilenode AS toast_relfilenode, i.indexrelid, "
                      "cti.relfilenode AS toast_index_relfilenode "
                      "FROM pg_catalog.pg_class c "
                      "LEFT JOIN pg_catalog.pg_index i ON (c.reltoastrelid = i.indrelid AND i.indisvalid) "
                      "LEFT JOIN pg_catalog.pg_class ct ON (c.reltoastrelid = ct.oid) "
                      "LEFT JOIN pg_catalog.pg_class AS cti ON (i.indexrelid = cti.oid) "
                      "WHERE c.oid = '%u'::pg_catalog.oid;",
                      pg_class_oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    // Extract values from query result
    relkind = *PQgetvalue(res, 0, PQfnumber(res, "relkind"));
    relfilenumber = atooid(PQgetvalue(res, 0, PQfnumber(res, "relfilenode")));
    toast_oid = atooid(PQgetvalue(res, 0, PQfnumber(res, "reltoastrelid")));
    toast_relfilenumber = atooid(PQgetvalue(res, 0, PQfnumber(res, "toast_relfilenode")));
    toast_index_oid = atooid(PQgetvalue(res, 0, PQfnumber(res, "indexrelid")));
    toast_index_relfilenumber = atooid(PQgetvalue(res, 0, PQfnumber(res, "toast_index_relfilenode")));

    if (!is_index) {
        // Handle table/heap relations
        appendPQExpBuffer(upgrade_buffer,
                          "SELECT pg_catalog.binary_upgrade_set_next_heap_pg_class_oid('%u'::pg_catalog.oid);\n",
                          pg_class_oid);

        // Preserve relfilenode if valid and not a partitioned table
        if (RelFileNumberIsValid(relfilenumber) && relkind != RELKIND_PARTITIONED_TABLE) {
            appendPQExpBuffer(upgrade_buffer,
                              "SELECT pg_catalog.binary_upgrade_set_next_heap_relfilenode('%u'::pg_catalog.oid);\n",
                              relfilenumber);
        }

        // Handle TOAST table and its index if they exist
        if (OidIsValid(toast_oid) && relkind != RELKIND_PARTITIONED_TABLE) {
            // Set TOAST table OID and relfilenode
            appendPQExpBuffer(upgrade_buffer,
                              "SELECT pg_catalog.binary_upgrade_set_next_toast_pg_class_oid('%u'::pg_catalog.oid);\n",
                              toast_oid);
            appendPQExpBuffer(upgrade_buffer,
                              "SELECT pg_catalog.binary_upgrade_set_next_toast_relfilenode('%u'::pg_catalog.oid);\n",
                              toast_relfilenumber);

            // Set TOAST index OID and relfilenode
            appendPQExpBuffer(upgrade_buffer,
                              "SELECT pg_catalog.binary_upgrade_set_next_index_pg_class_oid('%u'::pg_catalog.oid);\n",
                              toast_index_oid);
            appendPQExpBuffer(upgrade_buffer,
                              "SELECT pg_catalog.binary_upgrade_set_next_index_relfilenode('%u'::pg_catalog.oid);\n",
                              toast_index_relfilenumber);
        }
    } else {
        // Handle index relations
        appendPQExpBuffer(upgrade_buffer,
                          "SELECT pg_catalog.binary_upgrade_set_next_index_pg_class_oid('%u'::pg_catalog.oid);\n",
                          pg_class_oid);
        appendPQExpBuffer(upgrade_buffer,
                          "SELECT pg_catalog.binary_upgrade_set_next_index_relfilenode('%u'::pg_catalog.oid);\n",
                          relfilenumber);
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```