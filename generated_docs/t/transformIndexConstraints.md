# transformIndexConstraints

## Location
[src/backend/parser/parse_utilcmd.c:2058-2160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L2058-L2160)

## Overview
Handles UNIQUE, PRIMARY KEY, and EXCLUDE constraints that create indexes, merging in any index definitions from LIKE ... INCLUDING INDEXES clauses and removing redundant index specifications.

## Definition

```c
static void
transformIndexConstraints(CreateStmtContext *cxt)
```
## Detailed Description
The  function processes index-generating constraints during table creation or alteration. It transforms UNIQUE, PRIMARY KEY, and EXCLUDE constraints into corresponding IndexStmt nodes that will create the appropriate indexes. The function implements deduplication logic to remove redundant index specifications that might arise from overlapping constraints (e.g., when a column is marked both UNIQUE and PRIMARY KEY).

The function operates in two main phases:
1. **Constraint Processing**: Iterates through all index constraints in the context, calling  to convert each constraint into an IndexStmt
2. **Deduplication**: Removes redundant indexes by comparing index parameters, included parameters, WHERE clauses, exclude operators, access methods, and various index properties

Special handling ensures that PRIMARY KEY indexes are preserved in preference to other equivalent indexes, and named constraints transfer their names to previously unnamed equivalent indexes.

## Parameters / Member Variables
- `*cxt`: Pointer to CreateStmtContext containing the constraints to process and the target list for generated statements
## Dependencies
- Functions called/Symbols referenced:
  - [transformIndexConstraint](transformIndexConstraint.md) (converts individual constraints to IndexStmt)
  - [equal](../e/equal.md) (deep comparison of node structures)
  - [list_concat](../l/list_concat.md) (concatenates lists)
  - CreateStmtContext, IndexStmt, Constraint (data structures)
  - CONSTR_PRIMARY, CONSTR_UNIQUE, CONSTR_EXCLUSION (constraint type constants)
- Called from (representative examples):
  - [transformCreateStmt](transformCreateStmt.md) (during CREATE TABLE processing)
  - [transformAlterTableStmt](transformAlterTableStmt.md) (during ALTER TABLE processing)

## Notes and Other Information
- This is a static function in parse_utilcmd.c, part of the utility command parsing infrastructure
- Implements PostgreSQL's policy of allowing redundant constraints without error (e.g., UNIQUE PRIMARY KEY)
- The deduplication logic compares multiple index properties: parameters, included columns, WHERE clause, exclude operators, access method, nulls distinctness, deferrability
- PRIMARY KEY constraints receive special treatment and are always kept in the final index list
- Generated IndexStmt nodes are appended to the context's action list (cxt->alist) for later execution
- Supports both CREATE TABLE and ALTER TABLE scenarios through the same logic

## Simplified Source

```c
static void transformIndexConstraints(CreateStmtContext *cxt) {
    IndexStmt *index;
    List *indexlist = NIL;
    List *finalindexlist = NIL;

    // Transform each constraint into an IndexStmt
    foreach(lc, cxt->ixconstraints) {
        Constraint *constraint = lfirst_node(Constraint, lc);

        Assert(constraint->contype == CONSTR_PRIMARY ||
               constraint->contype == CONSTR_UNIQUE ||
               constraint->contype == CONSTR_EXCLUSION);

        index = transformIndexConstraint(constraint, cxt);
        indexlist = lappend(indexlist, index);
    }

    // Remove redundant index specifications
    if (cxt->pkey != NULL) {
        // Keep PRIMARY KEY index in preference to others
        finalindexlist = list_make1(cxt->pkey);
    }

    foreach(lc, indexlist) {
        bool keep = true;
        index = lfirst(lc);

        // Skip if it's already the primary key
        if (index == cxt->pkey)
            continue;

        // Check for duplicates with existing indexes
        foreach(k, finalindexlist) {
            IndexStmt *priorindex = lfirst(k);

            if (equal(index->indexParams, priorindex->indexParams) &&
                equal(index->indexIncludingParams, priorindex->indexIncludingParams) &&
                equal(index->whereClause, priorindex->whereClause) &&
                equal(index->excludeOpNames, priorindex->excludeOpNames) &&
                strcmp(index->accessMethod, priorindex->accessMethod) == 0 &&
                index->nulls_not_distinct == priorindex->nulls_not_distinct &&
                index->deferrable == priorindex->deferrable &&
                index->initdeferred == priorindex->initdeferred) {

                // Merge properties and transfer name if needed
                priorindex->unique |= index->unique;
                if (priorindex->idxname == NULL)
                    priorindex->idxname = index->idxname;
                keep = false;
                break;
            }
        }

        if (keep)
            finalindexlist = lappend(finalindexlist, index);
    }

    // Add all IndexStmts to the action list
    cxt->alist = list_concat(cxt->alist, finalindexlist);
}
```