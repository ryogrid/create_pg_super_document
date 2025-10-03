# transformIndexConstraint

## Location
[src/backend/parser/parse_utilcmd.c:2161-2696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L2161-L2696)

## Overview
Transforms a single UNIQUE, PRIMARY KEY, or EXCLUDE constraint into an IndexStmt, handling column validation, NOT NULL enforcement for primary keys, and existing index reuse scenarios.

## Definition

```c
static IndexStmt *
transformIndexConstraint(Constraint *constraint, CreateStmtContext *cxt)
```
## Detailed Description
The  function converts individual constraint definitions into corresponding index creation statements. This is a complex function that handles several distinct scenarios:

1. **Constraint Type Processing**: Handles UNIQUE, PRIMARY KEY, and EXCLUDE constraints, setting appropriate index properties (uniqueness, primary key flag, deferrability)

2. **Existing Index Reuse**: When  is specified, it validates that the existing index meets all requirements (uniqueness, correct access method, no expressions, etc.) and extracts column information from the index

3. **Column Validation**: For new constraints, it verifies that all referenced columns exist in the table definition, inherited tables, or system catalogs

4. **NOT NULL Enforcement**: For PRIMARY KEY constraints, it ensures columns are marked NOT NULL either by setting flags on new column definitions or generating ALTER TABLE SET NOT NULL commands

5. **Exclusion Constraint Handling**: Processes the special syntax for EXCLUDE constraints that pairs index elements with operator names

The function performs extensive validation to ensure constraint semantics are preserved and generates appropriate IndexStmt and AlterTableCmd nodes.

## Parameters / Member Variables
- `*constraint`: The Constraint node representing the UNIQUE, PRIMARY KEY, or EXCLUDE constraint to transform
- `*cxt`: The CreateStmtContext containing table definition information, existing columns, and action lists
## Dependencies
- Functions called/Symbols referenced:
  - [index_open](../i/index_open.md) (opens existing index for validation)
  - [get_index_constraint](../g/get_index_constraint.md) (checks if index already has a constraint)
  - [GetDefaultOpClass](../G/GetDefaultOpClass.md) (validates operator class requirements)
  - [get_relname_relid](../g/get_relname_relid.md) (looks up existing index by name)
  - [SystemAttributeByName](../S/SystemAttributeByName.md) (validates system column references)
  - [table_openrv](table_openrv.md) (opens inherited tables for column lookup)
  - [relation_close](../r/relation_close.md) (closes opened relations)
  - makeNode, makeString, copyObject (node construction utilities)
- Called from (representative examples):
  - [transformIndexConstraints](transformIndexConstraints.md) (processes all index constraints for a table)

## Notes and Other Information
- This is a static function in parse_utilcmd.c, part of the constraint transformation infrastructure
- Handles both CREATE TABLE and ALTER TABLE scenarios through the same logic
- Extensive validation for USING INDEX syntax ensures semantic equivalence with freshly created constraints
- Primary key constraints automatically enforce NOT NULL on all key columns
- Supports included columns (non-key columns stored in index for covering index functionality)
- Generates separate ALTER TABLE commands for runtime NOT NULL enforcement when needed
- Validates inheritance hierarchies when checking column existence
- Ensures compatibility with pg_dump/pg_restore by requiring exact matches for reused indexes

## Simplified Source

```c
static IndexStmt *transformIndexConstraint(Constraint *constraint, CreateStmtContext *cxt) {
    IndexStmt *index = makeNode(IndexStmt);
    List *notnullcmds = NIL;

    // Set basic index properties based on constraint type
    index->unique = (constraint->contype != CONSTR_EXCLUSION);
    index->primary = (constraint->contype == CONSTR_PRIMARY);
    index->nulls_not_distinct = constraint->nulls_not_distinct;
    index->isconstraint = true;
    index->deferrable = constraint->deferrable;
    index->initdeferred = constraint->initdeferred;

    // Check for multiple primary keys
    if (index->primary) {
        if (cxt->pkey != NULL)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errmsg("multiple primary keys for table \"%s\" are not allowed",
                                  cxt->relation->relname)));
        cxt->pkey = index;
    }

    // Set index properties from constraint
    index->idxname = constraint->conname ? pstrdup(constraint->conname) : NULL;
    index->relation = cxt->relation;
    index->accessMethod = constraint->access_method ? constraint->access_method : DEFAULT_INDEX_TYPE;
    index->options = constraint->options;
    index->tableSpace = constraint->indexspace;
    index->whereClause = constraint->where_clause;

    // Initialize lists
    index->indexParams = NIL;
    index->indexIncludingParams = NIL;
    index->excludeOpNames = NIL;

    // Handle USING INDEX case (existing index reuse)
    if (constraint->indexname != NULL) {
        // Validate existing index and extract column information
        // (Detailed validation logic simplified here)
        char *index_name = constraint->indexname;
        Oid index_oid = get_relname_relid(index_name, RelationGetNamespace(cxt->rel));

        if (!OidIsValid(index_oid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                           errmsg("index \"%s\" does not exist", index_name)));

        // Perform extensive validation on the existing index
        // Extract column information from existing index
        index->indexOid = index_oid;
    }

    // Handle EXCLUDE constraints
    if (constraint->contype == CONSTR_EXCLUSION) {
        foreach(lc, constraint->exclusions) {
            List *pair = (List *) lfirst(lc);
            IndexElem *elem = linitial_node(IndexElem, pair);
            List *opname = lsecond_node(List, pair);

            index->indexParams = lappend(index->indexParams, elem);
            index->excludeOpNames = lappend(index->excludeOpNames, opname);
        }
    }
    // Handle UNIQUE and PRIMARY KEY constraints
    else {
        // Process each key column
        foreach(lc, constraint->keys) {
            char *key = strVal(lfirst(lc));
            bool found = false;
            bool forced_not_null = false;

            // Check if column exists in new table definition
            foreach(columns, cxt->columns) {
                ColumnDef *column = lfirst_node(ColumnDef, columns);
                if (strcmp(column->colname, key) == 0) {
                    found = true;
                    // For primary keys, mark column as NOT NULL
                    if (constraint->contype == CONSTR_PRIMARY && !column->is_from_type) {
                        column->is_not_null = true;
                        forced_not_null = true;
                    }
                    break;
                }
            }

            // Check system columns and inherited tables if not found
            if (!found) {
                if (SystemAttributeByName(key) != NULL) {
                    found = true;
                } else if (cxt->inhRelations) {
                    // Search inherited tables for the column
                    foreach(inher, cxt->inhRelations) {
                        RangeVar *inh = lfirst_node(RangeVar, inher);
                        Relation rel = table_openrv(inh, AccessShareLock);
                        // Search for column in inherited table
                        table_close(rel, NoLock);
                        if (found) break;
                    }
                }
            }

            // Error if column not found in CREATE TABLE
            if (!found && !cxt->isalter)
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                               errmsg("column \"%s\" named in key does not exist", key)));

            // Create IndexElem for this column
            IndexElem *iparam = makeNode(IndexElem);
            iparam->name = pstrdup(key);
            iparam->expr = NULL;
            iparam->collation = NIL;
            iparam->opclass = NIL;
            iparam->ordering = SORTBY_DEFAULT;
            iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
            index->indexParams = lappend(index->indexParams, iparam);

            // Generate SET NOT NULL command if needed for primary key
            if (constraint->contype == CONSTR_PRIMARY && !forced_not_null) {
                AlterTableCmd *notnullcmd = makeNode(AlterTableCmd);
                notnullcmd->subtype = AT_SetNotNull;
                notnullcmd->name = pstrdup(key);
                notnullcmds = lappend(notnullcmds, notnullcmd);
            }
        }
    }

    // Process included columns
    foreach(lc, constraint->including) {
        char *key = strVal(lfirst(lc));
        // Similar column validation logic as above
        // Add to indexIncludingParams
        IndexElem *iparam = makeNode(IndexElem);
        iparam->name = pstrdup(key);
        index->indexIncludingParams = lappend(index->indexIncludingParams, iparam);
    }

    // Add NOT NULL commands to context if needed
    if (notnullcmds) {
        AlterTableStmt *alterstmt = makeNode(AlterTableStmt);
        alterstmt->relation = copyObject(cxt->relation);
        alterstmt->cmds = notnullcmds;
        alterstmt->objtype = OBJECT_TABLE;
        cxt->alist = lappend(cxt->alist, alterstmt);
    }

    return index;
}
```