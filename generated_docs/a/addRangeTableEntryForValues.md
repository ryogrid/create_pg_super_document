# addRangeTableEntryForValues

## Location
[src/backend/parser/parse_relation.c:2134-2215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L2134-L2215)

## Overview
Creates a range table entry (RTE) for a VALUES clause and adds it to the parser state's range table, returning a ParseNamespaceItem for the new entry.

## Definition
```c
ParseNamespaceItem *addRangeTableEntryForValues(ParseState *pstate,
                                               List *exprs,
                                               List *coltypes,
                                               List *coltypmods,
                                               List *colcollations,
                                               Alias *alias,
                                               bool lateral,
                                               bool inFromCl)
```

## Detailed Description
This function creates a RangeTblEntry of type RTE_VALUES for handling VALUES clauses in SQL statements. It constructs the RTE with provided column information, handles alias generation for unnamed columns, and builds a ParseNamespaceItem for namespace management. The function is similar to addRangeTableEntry() but specifically tailored for VALUES lists. It automatically generates column names ("column1", "column2", etc.) for any unspecified aliases and validates that the number of aliases matches the number of columns.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and range table
- `exprs`: List of expression lists representing the VALUES data
- `coltypes`: List of column data types for the VALUES clause
- `coltypmods`: List of column type modifiers
- `colcollations`: List of column collation information
- `alias`: Optional alias for the VALUES clause (uses "*VALUES*" if NULL)
- `lateral`: Boolean indicating if this is a lateral reference
- `inFromCl`: Boolean indicating if this appears in the FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RangeTblEntry creation)
  - copyObject (for alias copying)
  - [makeAlias](../m/makeAlias.md) (for default alias creation)
  - [makeString](../m/makeString.md) (for column name creation)
  - [buildNSItemFromLists](../b/buildNSItemFromLists.md) (for ParseNamespaceItem construction)
  - [lappend](../l/lappend.md) (for list operations)
- Called from (representative examples):
  - [transformInsertStmt](../t/transformInsertStmt.md) (in analyze.c:890)
  - [transformValuesClause](../t/transformValuesClause.md) (in analyze.c:1639)

## Notes and Other Information
- The function performs validation to ensure the number of column aliases matches the number of actual columns
- Access permissions are not checked for VALUES RTEs as they are considered similar to subqueries
- The caller is responsible for adding the returned ParseNamespaceItem to the appropriate namespace list
- Column names are automatically generated as "column1", "column2", etc. for unspecified aliases
- Located in src/backend/parser/parse_relation.c:2134-2215

## Simplified Source

```c
ParseNamespaceItem *addRangeTableEntryForValues(ParseState *pstate,
                                               List *exprs,
                                               List *coltypes,
                                               List *coltypmods,
                                               List *colcollations,
                                               Alias *alias,
                                               bool lateral,
                                               bool inFromCl) {
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    char *refname = alias ? alias->aliasname : pstrdup("*VALUES*");
    Alias *eref;
    int numaliases, numcolumns;

    Assert(pstate != NULL);

    // Set up VALUES RTE
    rte->rtekind = RTE_VALUES;
    rte->relid = InvalidOid;
    rte->subquery = NULL;
    rte->values_lists = exprs;
    rte->coltypes = coltypes;
    rte->coltypmods = coltypmods;
    rte->colcollations = colcollations;
    rte->alias = alias;

    // Create expanded alias with proper column names
    eref = alias ? copyObject(alias) : makeAlias(refname, NIL);

    // Fill in missing column aliases with generated names
    numcolumns = list_length((List *) linitial(exprs));
    numaliases = list_length(eref->colnames);
    while (numaliases < numcolumns) {
        char attrname[64];
        numaliases++;
        snprintf(attrname, sizeof(attrname), "column%d", numaliases);
        eref->colnames = lappend(eref->colnames,
                                makeString(pstrdup(attrname)));
    }

    // Validate alias count matches column count
    if (numcolumns < numaliases)
        ereport(ERROR, "VALUES lists have %d columns available but %d specified",
                numcolumns, numaliases);

    rte->eref = eref;

    // Set RTE flags
    rte->lateral = lateral;
    rte->inFromCl = inFromCl;

    // Add RTE to range table
    pstate->p_rtable = lappend(pstate->p_rtable, rte);

    // Build and return ParseNamespaceItem
    return buildNSItemFromLists(rte, list_length(pstate->p_rtable),
                               rte->coltypes, rte->coltypmods,
                               rte->colcollations);
}
```

**Key Points:**
- Creates RTE_VALUES range table entry for VALUES clauses
- Automatically generates column names ("column1", "column2", etc.) for unspecified aliases
- Validates that the number of aliases matches the number of columns
- Sets up proper RTE structure with types, modifiers, and collations
- Adds RTE to parser's range table and returns ParseNamespaceItem
- No permission checks needed (VALUES are like subqueries)
- Caller responsible for adding to namespace list if needed