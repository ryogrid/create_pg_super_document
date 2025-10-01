# addRangeTableEntryForCTE

## Location
[src/backend/parser/parse_relation.c:2314-2465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L2314-L2465)

## Overview
Creates a range table entry (RTE) for a Common Table Expression (CTE) reference and adds it to the parser state's range table, returning a ParseNamespaceItem for the new CTE entry.

## Definition
```c
ParseNamespaceItem *addRangeTableEntryForCTE(ParseState *pstate,
                                             CommonTableExpr *cte,
                                             Index levelsup,
                                             RangeVar *rv,
                                             bool inFromCl)
```

## Detailed Description
This function creates a RangeTblEntry of type RTE_CTE for handling Common Table Expression references in SQL statements. It manages CTE-specific metadata including self-reference detection, reference counting, and recursive CTE handling. The function validates that data-modifying CTEs (INSERT/UPDATE/DELETE/MERGE) have RETURNING clauses and handles special CTE features like SEARCH and CYCLE clauses that add additional columns. It automatically copies column type information from the CTE definition and manages alias resolution for column names.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and range table
- `cte`: CommonTableExpr structure containing the CTE definition and metadata
- `levelsup`: Number of nesting levels up to find the CTE definition (0 = current level)
- `rv`: RangeVar containing the reference information and optional alias
- `inFromCl`: Boolean indicating if this appears in the FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RangeTblEntry creation)
  - copyObject (for alias copying)
  - [makeAlias](../m/makeAlias.md) (for default alias creation)
  - [list_copy](../l/list_copy.md) (for copying CTE column information)
  - [makeString](../m/makeString.md) (for column name creation)
  - [lappend_oid](../l/lappend_oid.md), lappend_int (for column type management)
  - [buildNSItemFromLists](../b/buildNSItemFromLists.md) (for ParseNamespaceItem construction)
- Called from (representative examples):
  - [getNSItemForSpecialRelationTypes](../g/getNSItemForSpecialRelationTypes.md) (in parse_clause.c:1027)

## Notes and Other Information
- Automatically detects self-references by checking if CTE's parse analysis is completed
- Increments CTE reference count for non-self-referencing uses
- Validates that data-modifying CTEs have RETURNING clauses (except for self-references)
- Handles SEARCH clause by adding a search sequence column (RECORD or RECORDARRAY type)
- Handles CYCLE clause by adding cycle mark and cycle path columns
- SEARCH and CYCLE clause columns are marked as non-expandable in star expansion for nested queries
- Access permissions are not checked for CTE RTEs as they are treated like subqueries
- Self-references are only allowed for recursive CTEs
- Located in src/backend/parser/parse_relation.c:2314-2465

## Simplified Source

```c
ParseNamespaceItem *
addRangeTableEntryForCTE(ParseState *pstate,
                         CommonTableExpr *cte,
                         Index levelsup,
                         RangeVar *rv,
                         bool inFromCl)
{
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    Alias *alias = rv->alias;
    char *refname = alias ? alias->aliasname : cte->ctename;
    Alias *eref;
    int numaliases, varattno;
    ListCell *lc;
    int n_dontexpand_columns = 0;
    ParseNamespaceItem *psi;

    // Set up basic RTE properties
    rte->rtekind = RTE_CTE;
    rte->ctename = cte->ctename;
    rte->ctelevelsup = levelsup;

    // Check for self-reference and update reference count
    rte->self_reference = !IsA(cte->ctequery, Query);
    if (!rte->self_reference)
        cte->cterefcount++;

    // Validate data-modifying CTEs have RETURNING clause
    if (IsA(cte->ctequery, Query))
    {
        Query *ctequery = (Query *) cte->ctequery;
        if (ctequery->commandType != CMD_SELECT && ctequery->returningList == NIL)
            ereport(ERROR, "WITH query \"%s\" needs RETURNING clause", cte->ctename);
    }

    // Copy column type information from CTE definition
    rte->coltypes = list_copy(cte->ctecoltypes);
    rte->coltypmods = list_copy(cte->ctecoltypmods);
    rte->colcollations = list_copy(cte->ctecolcollations);

    // Set up column aliases
    rte->alias = alias;
    eref = alias ? copyObject(alias) : makeAlias(refname, NIL);
    numaliases = list_length(eref->colnames);

    // Fill in missing alias columns from CTE definition
    varattno = 0;
    foreach(lc, cte->ctecolnames)
    {
        varattno++;
        if (varattno > numaliases)
            eref->colnames = lappend(eref->colnames, lfirst(lc));
    }

    // Validate alias count
    if (varattno < numaliases)
        ereport(ERROR, "table \"%s\" has %d columns but %d aliases specified",
                refname, varattno, numaliases);

    rte->eref = eref;

    // Handle SEARCH clause if present
    if (cte->search_clause)
    {
        rte->eref->colnames = lappend(rte->eref->colnames,
                                    makeString(cte->search_clause->search_seq_column));
        rte->coltypes = lappend_oid(rte->coltypes,
                                  cte->search_clause->search_breadth_first ? RECORDOID : RECORDARRAYOID);
        rte->coltypmods = lappend_int(rte->coltypmods, -1);
        rte->colcollations = lappend_oid(rte->colcollations, InvalidOid);
        n_dontexpand_columns++;
    }

    // Handle CYCLE clause if present
    if (cte->cycle_clause)
    {
        // Add cycle mark column
        rte->eref->colnames = lappend(rte->eref->colnames,
                                    makeString(cte->cycle_clause->cycle_mark_column));
        rte->coltypes = lappend_oid(rte->coltypes, cte->cycle_clause->cycle_mark_type);
        rte->coltypmods = lappend_int(rte->coltypmods, cte->cycle_clause->cycle_mark_typmod);
        rte->colcollations = lappend_oid(rte->colcollations, cte->cycle_clause->cycle_mark_collation);

        // Add cycle path column
        rte->eref->colnames = lappend(rte->eref->colnames,
                                    makeString(cte->cycle_clause->cycle_path_column));
        rte->coltypes = lappend_oid(rte->coltypes, RECORDARRAYOID);
        rte->coltypmods = lappend_int(rte->coltypmods, -1);
        rte->colcollations = lappend_oid(rte->colcollations, InvalidOid);
        n_dontexpand_columns += 2;
    }

    rte->lateral = false;
    rte->inFromCl = inFromCl;

    // Add RTE to range table
    pstate->p_rtable = lappend(pstate->p_rtable, rte);

    // Build namespace item
    psi = buildNSItemFromLists(rte, list_length(pstate->p_rtable),
                               rte->coltypes, rte->coltypmods, rte->colcollations);

    // Mark SEARCH/CYCLE columns as non-expandable for nested queries
    if (rte->ctelevelsup > 0)
        for (int i = 0; i < n_dontexpand_columns; i++)
            psi->p_nscolumns[list_length(psi->p_names->colnames) - 1 - i].p_dontexpand = true;

    return psi;
}
```