# flagInhAttrs

## Location
[src/bin/pg_dump/common.c:501-645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L501-L645)

## Overview
Identifies inherited column attributes and optimizes their representation in dump output to avoid redundancy and ensure proper restoration.

## Definition

```c
static void
flagInhAttrs(Archive *fout, TableInfo *tblinfo, int numTables)
```
## Detailed Description
The flagInhAttrs function analyzes inheritance relationships between tables to optimize how column attributes are represented in the pg_dump output. It performs three critical optimizations: (1) flags columns that inherit NOT NULL constraints from parents to avoid redundant specifications, (2) creates explicit DEFAULT NULL clauses for child columns that need to override inherited non-null defaults, and (3) suppresses generation expressions in child tables when they match all parent generation expressions, improving compatibility with pre-v16 PostgreSQL servers.

The function carefully handles the complex inheritance semantics of PostgreSQL, where child tables can inherit constraints, defaults, and generation expressions from their parents. By identifying which attributes are truly inherited versus locally defined, it ensures that the dump output is both minimal and semantically correct during restoration.

## Parameters / Member Variables
- `*fout`: Archive structure containing database connection and dump configuration options
- `*tblinfo`: Array of TableInfo structures representing all tables in the database
- `numTables`: Number of tables in the tblinfo array
## Dependencies
- Functions called/Symbols referenced:
  - [strInArray](../s/strInArray.md) (searches for matching column names in parent tables)
  - [AssignDumpId](../A/AssignDumpId.md) (assigns dump IDs to manufactured AttrDefInfo objects)
  - [shouldPrintColumn](../s/shouldPrintColumn.md) (determines if column will be explicitly dumped)
  - [addObjectDependency](../a/addObjectDependency.md) (establishes dependencies for separate default clauses)
  - pg_malloc_object (memory allocation for AttrDefInfo)
- Called from (representative examples):
  - [getSchemaData](../g/getSchemaData.md) (src/bin/pg_dump/common.c:233)

## Notes and Other Information
The function processes tables in OID order but cannot assume parents are visited before children, requiring careful state management to avoid altering properties that affect other iterations. It creates synthetic AttrDefInfo objects for DEFAULT NULL clauses when children need to explicitly override inherited non-null defaults.

Special handling exists for generation expressions: they are suppressed in child tables only when all parents have identical expressions, except for partitions and binary upgrade mode where explicit specification is required. The function only processes regular tables, excluding sequences, views, and materialized views which don't participate in inheritance.

## Simplified Source

```c
static void flagInhAttrs(Archive *fout, TableInfo *tblinfo, int numTables) {
    DumpOptions *dopt = fout->dopt;
    int i, j, k;

    // Process each table in OID order
    for (i = 0; i < numTables; i++) {
        TableInfo *tbinfo = &(tblinfo[i]);
        int numParents;
        TableInfo **parents;

        // Skip tables that don't participate in inheritance
        if (tbinfo->relkind == RELKIND_SEQUENCE ||
            tbinfo->relkind == RELKIND_VIEW ||
            tbinfo->relkind == RELKIND_MATVIEW)
            continue;

        // Skip non-target tables
        if (!tbinfo->dobj.dump)
            continue;

        numParents = tbinfo->numParents;
        parents = tbinfo->parents;

        // Skip tables without parents
        if (numParents == 0)
            continue;

        // Process each column for inheritance attributes
        for (j = 0; j < tbinfo->numatts; j++) {
            bool foundNotNull = false;     // Attr was NOT NULL in a parent
            bool foundDefault = false;     // Found a default in a parent
            bool foundSameGenerated = false; // Found matching GENERATED
            bool foundDiffGenerated = false; // Found non-matching GENERATED

            // Skip dropped columns
            if (tbinfo->attisdropped[j])
                continue;

            // Check each parent for matching column names
            for (k = 0; k < numParents; k++) {
                TableInfo *parent = parents[k];
                int inhAttrInd;

                inhAttrInd = strInArray(tbinfo->attnames[j], parent->attnames, parent->numatts);
                if (inhAttrInd >= 0) {
                    AttrDefInfo *parentDef = parent->attrdefs[inhAttrInd];

                    // Check for inherited NOT NULL
                    foundNotNull |= parent->notnull[inhAttrInd];

                    // Check for inherited non-null default
                    foundDefault |= (parentDef != NULL &&
                                   strcmp(parentDef->adef_expr, "NULL") != 0 &&
                                   !parent->attgenerated[inhAttrInd]);

                    // Check for generated expressions
                    if (parent->attgenerated[inhAttrInd]) {
                        if (parentDef != NULL && tbinfo->attrdefs[j] != NULL &&
                            strcmp(parentDef->adef_expr, tbinfo->attrdefs[j]->adef_expr) == 0)
                            foundSameGenerated = true;
                        else
                            foundDiffGenerated = true;
                    }
                }
            }

            // Remember inherited NOT NULL
            tbinfo->inhNotNull[j] = foundNotNull;

            // Create DEFAULT NULL clause if child needs to override inherited default
            if (foundDefault && tbinfo->attrdefs[j] == NULL) {
                AttrDefInfo *attrDef;

                attrDef = pg_malloc_object(AttrDefInfo);
                attrDef->dobj.objType = DO_ATTRDEF;
                attrDef->dobj.catId.tableoid = 0;
                attrDef->dobj.catId.oid = 0;
                AssignDumpId(&attrDef->dobj);
                attrDef->dobj.name = pg_strdup(tbinfo->dobj.name);
                attrDef->dobj.namespace = tbinfo->dobj.namespace;
                attrDef->dobj.dump = tbinfo->dobj.dump;

                attrDef->adtable = tbinfo;
                attrDef->adnum = j + 1;
                attrDef->adef_expr = pg_strdup("NULL");

                // Handle dependency based on whether column will be dumped
                if (shouldPrintColumn(dopt, tbinfo, j)) {
                    attrDef->separate = false;
                } else {
                    attrDef->separate = true;
                    addObjectDependency(&attrDef->dobj, tbinfo->dobj.dumpId);
                }

                tbinfo->attrdefs[j] = attrDef;
            }

            // Suppress generation expression if inheritable (compatibility optimization)
            if (foundSameGenerated && !foundDiffGenerated &&
                !tbinfo->ispartition && !dopt->binary_upgrade)
                tbinfo->attrdefs[j]->dobj.dump = DUMP_COMPONENT_NONE;
        }
    }
}
```