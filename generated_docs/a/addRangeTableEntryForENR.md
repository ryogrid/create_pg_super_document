# addRangeTableEntryForENR

## Location
[src/backend/parser/parse_relation.c:2466-2574](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L2466-L2574)

## Overview
Creates a range table entry (RTE) for an Ephemeral Named Relation (ENR) reference and adds it to the parser state's range table, returning a ParseNamespaceItem for the new ENR entry.

## Definition
```c
ParseNamespaceItem *addRangeTableEntryForENR(ParseState *pstate,
                                             RangeVar *rv,
                                             bool inFromCl)
```

## Detailed Description
This function creates a RangeTblEntry for Ephemeral Named Relations, which are temporary named relations that exist only during query execution (such as transition tables in triggers). It looks up the ENR metadata from the QueryEnvironment and creates the appropriate RTE type based on the ENR type. Currently supports ENR_NAMED_TUPLESTORE types which become RTE_NAMEDTUPLESTORE entries. The function extracts column type information from the tuple descriptor and handles dropped columns by recording invalid OIDs. It also records dependency information to enable plan invalidation when referenced tables are altered.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and range table
- `rv`: RangeVar containing the ENR reference information and optional alias
- `inFromCl`: Boolean indicating if this appears in the FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RangeTblEntry creation)
  - [get_visible_ENR](../g/get_visible_ENR.md) (to retrieve ENR metadata from query environment)
  - [ENRMetadataGetTupDesc](../E/ENRMetadataGetTupDesc.md) (to get tuple descriptor from ENR metadata)
  - [makeAlias](../m/makeAlias.md) (for alias creation)
  - [buildRelationAliases](../b/buildRelationAliases.md) (for column alias resolution)
  - [lappend_oid](../l/lappend_oid.md), lappend_int (for column type management)
  - [buildNSItemFromTupleDesc](../b/buildNSItemFromTupleDesc.md) (for ParseNamespaceItem construction)
  - TupleDescAttr (for accessing tuple descriptor attributes)
- Called from (representative examples):
  - [getNSItemForSpecialRelationTypes](../g/getNSItemForSpecialRelationTypes.md) (in parse_clause.c:1029)

## Notes and Other Information
- Currently only supports ENR_NAMED_TUPLESTORE type, with extensibility for future ENR types
- Records dependency on the underlying relation (enrmd->reliddesc) for plan invalidation
- Handles dropped columns by recording InvalidOid in type information lists
- Validates that non-dropped columns have valid type OIDs
- Access permissions are not checked for ENR RTEs as they are temporary constructs
- ENRs are never lateral references (lateral = false)
- Stores additional ENR-specific metadata including name and tuple count
- Used primarily for transition tables in trigger contexts and similar ephemeral relations
- Located in src/backend/parser/parse_relation.c:2466-2574

## Simplified Source

```c
ParseNamespaceItem *
addRangeTableEntryForENR(ParseState *pstate,
                         RangeVar *rv,
                         bool inFromCl)
{
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    Alias *alias = rv->alias;
    char *refname = alias ? alias->aliasname : rv->relname;
    EphemeralNamedRelationMetadata enrmd;
    TupleDesc tupdesc;
    int attno;

    // Get ENR metadata from query environment
    enrmd = get_visible_ENR(pstate, rv->relname);

    // Set RTE type based on ENR type
    switch (enrmd->enrtype)
    {
        case ENR_NAMED_TUPLESTORE:
            rte->rtekind = RTE_NAMEDTUPLESTORE;
            break;
        default:
            elog(ERROR, "unexpected enrtype: %d", enrmd->enrtype);
            return NULL;
    }

    // Record dependency for plan invalidation
    rte->relid = enrmd->reliddesc;

    // Build column aliases and type information
    tupdesc = ENRMetadataGetTupDesc(enrmd);
    rte->eref = makeAlias(refname, NIL);
    buildRelationAliases(tupdesc, alias, rte->eref);

    // Store ENR-specific metadata
    rte->enrname = enrmd->name;
    rte->enrtuples = enrmd->enrtuples;
    rte->coltypes = NIL;
    rte->coltypmods = NIL;
    rte->colcollations = NIL;

    // Extract column type information from tuple descriptor
    for (attno = 1; attno <= tupdesc->natts; ++attno)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, attno - 1);

        if (att->attisdropped)
        {
            // Record invalid values for dropped columns
            rte->coltypes = lappend_oid(rte->coltypes, InvalidOid);
            rte->coltypmods = lappend_int(rte->coltypmods, 0);
            rte->colcollations = lappend_oid(rte->colcollations, InvalidOid);
        }
        else
        {
            // Record actual type information for active columns
            rte->coltypes = lappend_oid(rte->coltypes, att->atttypid);
            rte->coltypmods = lappend_int(rte->coltypmods, att->atttypmod);
            rte->colcollations = lappend_oid(rte->colcollations, att->attcollation);
        }
    }

    rte->lateral = false;
    rte->inFromCl = inFromCl;

    // Add RTE to range table
    pstate->p_rtable = lappend(pstate->p_rtable, rte);

    // Build and return namespace item
    return buildNSItemFromTupleDesc(rte, list_length(pstate->p_rtable), NULL, tupdesc);
}
```