# flagInhTables

## Location
[src/bin/pg_dump/common.c:293-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L293-L410)

## Overview
Establishes parent-child relationships for inheritance hierarchies and marks parent tables as interesting for dump processing.

## Definition

```c
static void
flagInhTables(Archive *fout, TableInfo *tblinfo, int numTables,
			  InhInfo *inhinfo, int numInherits)
```
## Detailed Description
The flagInhTables function processes PostgreSQL table inheritance information to establish proper parent-child relationships within the TableInfo structures. It serves two primary purposes: first, it creates bidirectional links between child tables and their parent tables by populating the parents array in each child TableInfo; second, it marks parent tables of dumpable tables as 'interesting' so they will be processed during subsequent phases like getTableAttrs and getIndexes.

The function also handles partition table attachments by creating TableAttachInfo objects for partitioned tables. These objects represent the ATTACH PARTITION operations needed to properly recreate the partitioning structure during database restoration. The function ensures proper dependency ordering by making TableAttachInfo objects depend on both the partition table and its parent table.

## Parameters / Member Variables
- `*fout`: Archive structure containing database connection and dump configuration
- `*tblinfo`: Array of TableInfo structures representing all tables in the database
- `numTables`: Number of tables in the tblinfo array
- `*inhinfo`: Array of InhInfo structures containing inheritance relationship data from pg_inherits
- `numInherits`: Number of inheritance relationships in the inhinfo array
## Dependencies
- Functions called/Symbols referenced:
  - [findTableByOid](findTableByOid.md) (locates TableInfo by OID)
  - [AssignDumpId](../A/AssignDumpId.md) (assigns unique dump IDs to objects)
  - [addObjectDependency](../a/addObjectDependency.md) (establishes dump order dependencies)
  - pg_realloc_array, pg_malloc_array (memory management)
- Called from (representative examples):
  - [getSchemaData](../g/getSchemaData.md) (src/bin/pg_dump/common.c:227)

## Notes and Other Information
The function includes performance optimizations by caching the last-used child and parent TableInfo pointers to avoid repeated hash table lookups when processing consecutive inheritance records for the same tables. Only direct ancestors of target tables are marked as interesting, which is sufficient for pg_dump's needs since inherited attributes don't require special handling beyond ensuring the parent structure exists.

For partitioned tables, the function creates TableAttachInfo objects that will generate ATTACH PARTITION commands during the dump restoration process. These objects have explicit dependencies on both the parent and child tables to ensure proper creation order during restoration.

## Simplified Source

```c
static void flagInhTables(Archive *fout, TableInfo *tblinfo, int numTables,
                         InhInfo *inhinfo, int numInherits) {
    TableInfo *child = NULL;
    TableInfo *parent = NULL;
    int i, j;

    // Set up parent-child relationships from inheritance info
    for (i = 0; i < numInherits; i++) {
        // Find child table (cache optimization)
        if (child == NULL || child->dobj.catId.oid != inhinfo[i].inhrelid) {
            child = findTableByOid(inhinfo[i].inhrelid);
            // Skip if no TableInfo found (likely partitioned index)
            if (child == NULL)
                continue;
        }

        // Find parent table (cache optimization)
        if (parent == NULL || parent->dobj.catId.oid != inhinfo[i].inhparent) {
            parent = findTableByOid(inhinfo[i].inhparent);
            if (parent == NULL)
                pg_fatal("failed sanity check, parent OID %u of table \"%s\" (OID %u) not found",
                        inhinfo[i].inhparent, child->dobj.name, child->dobj.catId.oid);
        }

        // Add parent to child's parent list
        if (child->numParents > 0)
            child->parents = pg_realloc_array(child->parents, TableInfo *, child->numParents + 1);
        else
            child->parents = pg_malloc_array(TableInfo *, 1);
        child->parents[child->numParents++] = parent;
    }

    // Mark parents as interesting and create partition attach info
    for (i = 0; i < numTables; i++) {
        // Mark direct parents of dumpable tables as interesting
        if (tblinfo[i].dobj.dump) {
            for (j = 0; j < tblinfo[i].numParents; j++)
                tblinfo[i].parents[j]->interesting = true;
        }

        // Create TableAttachInfo for partitions
        if ((tblinfo[i].dobj.dump & DUMP_COMPONENT_DEFINITION) && tblinfo[i].ispartition) {
            TableAttachInfo *attachinfo;

            // Partitions must have exactly one parent
            if (tblinfo[i].numParents != 1)
                pg_fatal("invalid number of parents %d for table \"%s\"",
                        tblinfo[i].numParents, tblinfo[i].dobj.name);

            // Create attach info object
            attachinfo = (TableAttachInfo *) palloc(sizeof(TableAttachInfo));
            attachinfo->dobj.objType = DO_TABLE_ATTACH;
            attachinfo->dobj.catId.tableoid = 0;
            attachinfo->dobj.catId.oid = 0;
            AssignDumpId(&attachinfo->dobj);
            attachinfo->dobj.name = pg_strdup(tblinfo[i].dobj.name);
            attachinfo->dobj.namespace = tblinfo[i].dobj.namespace;
            attachinfo->parentTbl = tblinfo[i].parents[0];
            attachinfo->partitionTbl = &tblinfo[i];

            // Set dependencies on both partition and parent tables
            addObjectDependency(&attachinfo->dobj, tblinfo[i].dobj.dumpId);
            addObjectDependency(&attachinfo->dobj, tblinfo[i].parents[0]->dobj.dumpId);
        }
    }
}
```