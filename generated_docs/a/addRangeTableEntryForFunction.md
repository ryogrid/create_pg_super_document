# addRangeTableEntryForFunction

## Location
[src/backend/parser/parse_relation.c:1734-2048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1734-L2048)

## Overview
Creates a range table entry for one or more functions in a FROM clause, handling complex type resolution, column definition validation, and tuple descriptor construction for function return types.

## Definition

```c
struct a tupdesc and fill
			 * in the RangeTblFunction's lists.  Limit number of columns to
			 * MaxHeapAttributeNumber, because CheckAttributeNamesTypes will.
			 */
			if (list_length(coldeflist) > MaxHeapAttributeNumber)
				ereport(ERROR,
						(errcode(ERRCODE_TOO_MANY_COLUMNS),
						 errmsg("column definition lists can have at most %d entries",
								MaxHeapAttributeNumber),
						 parser_errposition(pstate,
											exprLocation((Node *) coldeflist))));
```
## Detailed Description
The  function handles the complex task of creating range table entries for functions used in FROM clauses. This includes:

1. **Multiple Function Support**: Can handle multiple functions in a single RTE (e.g., )
2. **Type Resolution**: Determines whether functions return scalar, composite, or record types
3. **Column Definition Validation**: Enforces rules about when column definition lists are required/prohibited
4. **Tuple Descriptor Construction**: Creates appropriate tuple descriptors based on function return types
5. **Ordinality Column Support**: Adds ordinality columns when WITH ORDINALITY is specified
6. **Alias Processing**: Handles column aliases and auto-generates names when needed

The function performs extensive validation:
- Column definition lists are required for functions returning RECORD type
- Column definition lists are prohibited for functions with predetermined types
- Validates column count limits (MaxHeapAttributeNumber for individual functions, MaxTupleAttributeNumber for merged results)
- Ensures proper type compatibility and naming conventions

For functions returning different types:
- **Scalar types**: Creates single-column tuple descriptor
- **Composite types**: Uses existing tuple descriptor from the type
- **RECORD types**: Constructs tuple descriptor from column definition list

## Parameters / Member Variables
- : Parser state containing the range table and other parsing context
- : List of function names (used for error messages and auto-aliasing)
- : List of function call expressions to be evaluated
- : List of column definition lists (one per function, may contain NULLs)
- : RangeFunction node containing alias and ordinality information
- : Boolean indicating whether this is a LATERAL function reference
- : Boolean indicating whether this entry originates from a FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RTE and RangeTblFunction creation)
  - [makeAlias](../m/makeAlias.md) (for alias creation)
  - [get_expr_result_type](../g/get_expr_result_type.md) (function type analysis)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md) (tuple descriptor creation)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md), TupleDescInitEntryCollation (attribute initialization)
  - [TupleDescCopyEntry](../T/TupleDescCopyEntry.md) (for merging multiple function results)
  - [chooseScalarFunctionAlias](../c/chooseScalarFunctionAlias.md) (scalar function naming)
  - [typenameTypeIdAndMod](../t/typenameTypeIdAndMod.md) (type resolution from column definitions)
  - [CheckAttributeNamesTypes](../C/CheckAttributeNamesTypes.md) (column validation)
  - [buildRelationAliases](../b/buildRelationAliases.md) (alias processing)
  - Various list manipulation functions (lappend, lappend_oid, lappend_int)
- Called from (representative examples):
  - [transformRangeFunction](../t/transformRangeFunction.md) (in parse_clause.c)

## Notes and Other Information
- Functions are never checked for access rights by the permission system since they represent computed results
- The function supports PostgreSQL's SETOF functions and handles ordinality columns for row numbering
- [Complex](../C/Complex.md) error handling provides specific messages for different column definition list validation failures
- When multiple functions are present, their tuple descriptors are merged into a single composite descriptor
- The function name list is used primarily for error reporting and automatic alias generation
- LATERAL functions have special scoping rules allowing them to reference columns from preceding FROM items
- Column definition lists have strict limits to prevent exceeding PostgreSQL's tuple attribute limits
- Type resolution handles pseudo-types like RECORD with special validation rules

## Simplified Source

```c
ParseNamespaceItem *
addRangeTableEntryForFunction(ParseState *pstate,
                              List *funcnames,
                              List *funcexprs,
                              List *coldeflists,
                              RangeFunction *rangefunc,
                              bool lateral,
                              bool inFromCl)
{
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    Alias *alias = rangefunc->alias;
    char *aliasname;
    int nfuncs = list_length(funcexprs);
    TupleDesc *functupdescs;
    TupleDesc tupdesc;
    int totalatts = 0;
    int funcno = 0;

    // Set up basic RTE properties
    rte->rtekind = RTE_FUNCTION;
    rte->relid = InvalidOid;
    rte->functions = NIL;
    rte->funcordinality = rangefunc->ordinality;
    rte->alias = alias;

    // Choose alias name (first function name if no explicit alias)
    aliasname = alias ? alias->aliasname : linitial(funcnames);
    rte->eref = makeAlias(aliasname, NIL);

    // Process each function
    functupdescs = palloc(nfuncs * sizeof(TupleDesc));

    forthree(lc1, funcexprs, lc2, funcnames, lc3, coldeflists)
    {
        Node *funcexpr = lfirst(lc1);
        char *funcname = lfirst(lc2);
        List *coldeflist = lfirst(lc3);
        RangeTblFunction *rtfunc = makeNode(RangeTblFunction);
        TypeFuncClass functypclass;
        Oid funcrettype;

        // Determine function return type
        functypclass = get_expr_result_type(funcexpr, &funcrettype, &tupdesc);

        // Validate column definition list requirements
        if (coldeflist != NIL && functypclass != TYPEFUNC_RECORD)
            ereport(ERROR, "column definition list not allowed for this function type");
        else if (coldeflist == NIL && functypclass == TYPEFUNC_RECORD)
            ereport(ERROR, "column definition list required for RECORD functions");

        // Create tuple descriptor based on function type
        if (functypclass == TYPEFUNC_SCALAR)
        {
            // Single column for scalar return
            tupdesc = CreateTemplateTupleDesc(1);
            TupleDescInitEntry(tupdesc, 1,
                             chooseScalarFunctionAlias(funcexpr, funcname, alias, nfuncs),
                             funcrettype, exprTypmod(funcexpr), 0);
        }
        else if (functypclass == TYPEFUNC_RECORD)
        {
            // Build from column definition list
            tupdesc = CreateTemplateTupleDesc(list_length(coldeflist));
            int i = 1;
            foreach(col, coldeflist)
            {
                ColumnDef *n = lfirst(col);
                Oid attrtype;
                int32 attrtypmod;
                typenameTypeIdAndMod(pstate, n->typeName, &attrtype, &attrtypmod);
                TupleDescInitEntry(tupdesc, i++, n->colname, attrtype, attrtypmod, 0);
            }
            CheckAttributeNamesTypes(tupdesc, RELKIND_COMPOSITE_TYPE, CHKATYPE_ANYRECORD);
        }
        // COMPOSITE types use existing tupdesc from get_expr_result_type

        rtfunc->funcexpr = funcexpr;
        rtfunc->funccolcount = tupdesc->natts;
        rte->functions = lappend(rte->functions, rtfunc);

        functupdescs[funcno++] = tupdesc;
        totalatts += tupdesc->natts;
    }

    // Merge multiple function results if needed
    if (nfuncs > 1 || rangefunc->ordinality)
    {
        if (rangefunc->ordinality) totalatts++;

        tupdesc = CreateTemplateTupleDesc(totalatts);
        int natts = 0;

        // Copy all function columns
        for (int i = 0; i < nfuncs; i++)
            for (int j = 1; j <= functupdescs[i]->natts; j++)
                TupleDescCopyEntry(tupdesc, ++natts, functupdescs[i], j);

        // Add ordinality column if requested
        if (rangefunc->ordinality)
            TupleDescInitEntry(tupdesc, ++natts, "ordinality", INT8OID, -1, 0);
    }
    else
    {
        tupdesc = functupdescs[0];
    }

    buildRelationAliases(tupdesc, alias, rte->eref);
    rte->lateral = lateral;
    rte->inFromCl = inFromCl;

    pstate->p_rtable = lappend(pstate->p_rtable, rte);

    return buildNSItemFromTupleDesc(rte, list_length(pstate->p_rtable),
                                    NULL, tupdesc);
}
```