# analyzeCTETargetList

## Location
[src/backend/parser/parse_cte.c:571-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L571-L647)

## Overview
Computes derived fields of a CTE including column names, types, type modifiers, and collations from the transformed output target list.

## Definition

```c
void
analyzeCTETargetList(ParseState *pstate, CommonTableExpr *cte, List *tlist)
```
## Detailed Description
This function determines the final column specifications for a CTE based on its transformed target list. It handles several critical aspects:

1. **Column naming**: Uses alias column names when provided, falling back to target entry names for additional columns
2. **Type determination**: Extracts data types, type modifiers, and collations from target list expressions
3. **Unknown type resolution**: For recursive CTEs, converts UNKNOWN type columns to TEXT to ensure type consistency
4. **Validation**: Ensures that the number of available columns matches or exceeds the number of specified aliases

The function is called at different stages depending on CTE type:
- For non-recursive CTEs: Called after transforming the entire query
- For recursive CTEs: Called after transforming only the non-recursive term to establish baseline types

## Parameters / Member Variables
- `*pstate`: Parse state used primarily for error message context and location information
- `*cte`: The CommonTableExpr node whose derived fields need to be computed
- `*tlist`: The transformed target list from which to derive column information
## Dependencies
- Functions called/Symbols referenced:
  - copyObject - creates copy of alias column names
  - [makeString](../m/makeString.md) - creates string nodes for column names
  - [exprType](../e/exprType.md) - extracts data type from expressions
  - [exprTypmod](../e/exprTypmod.md) - extracts type modifier from expressions
  - [exprCollation](../e/exprCollation.md) - extracts collation from expressions
  - [lappend_oid](../l/lappend_oid.md) - appends OID values to lists
  - [lappend_int](../l/lappend_int.md) - appends integer values to lists
- Called from (representative examples):
  - [analyzeCTE](analyzeCTE.md) - for non-recursive CTEs after query transformation
  - [determineRecursiveColTypes](../d/determineRecursiveColTypes.md) - for recursive CTEs after analyzing non-recursive term

## Notes and Other Information
- Fills in cte->ctecolnames, cte->ctecoltypes, cte->ctecoltypmods, and cte->ctecolcollations
- Allows alias lists to be shorter than the actual column count (PostgreSQL extension)
- For recursive CTEs, forces UNKNOWN columns to TEXT type with default collation
- Preserves existing collations even when converting UNKNOWN to TEXT
- Validates that enough columns are available to satisfy all specified aliases
- Skips junk entries in the target list during processing

## Simplified Source

```c
void
analyzeCTETargetList(ParseState *pstate, CommonTableExpr *cte, List *tlist)
{
    int numaliases;
    int varattno;
    ListCell *tlistitem;

    // Initialize CTE column metadata
    cte->ctecolnames = copyObject(cte->aliascolnames);
    cte->ctecoltypes = cte->ctecoltypmods = cte->ctecolcollations = NIL;
    numaliases = list_length(cte->aliascolnames);
    varattno = 0;

    // Process each target list entry
    foreach(tlistitem, tlist)
    {
        TargetEntry *te = (TargetEntry *) lfirst(tlistitem);
        Oid coltype;
        int32 coltypmod;
        Oid colcoll;

        // Skip junk entries
        if (te->resjunk)
            continue;

        varattno++;

        // Add column name if beyond alias count
        if (varattno > numaliases)
        {
            char *attrname = pstrdup(te->resname);
            cte->ctecolnames = lappend(cte->ctecolnames, makeString(attrname));
        }

        // Extract type information
        coltype = exprType((Node *) te->expr);
        coltypmod = exprTypmod((Node *) te->expr);
        colcoll = exprCollation((Node *) te->expr);

        // For recursive CTEs, convert UNKNOWN to TEXT
        if (cte->cterecursive && coltype == UNKNOWNOID)
        {
            coltype = TEXTOID;
            coltypmod = -1;
            if (!OidIsValid(colcoll))
                colcoll = DEFAULT_COLLATION_OID;
        }

        // Store type information
        cte->ctecoltypes = lappend_oid(cte->ctecoltypes, coltype);
        cte->ctecoltypmods = lappend_int(cte->ctecoltypmods, coltypmod);
        cte->ctecolcollations = lappend_oid(cte->ctecolcollations, colcoll);
    }

    // Validate column count
    if (varattno < numaliases)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                 errmsg("WITH query \"%s\" has %d columns available but %d columns specified",
                        cte->ctename, varattno, numaliases),
                 parser_errposition(pstate, cte->location)));
}
```