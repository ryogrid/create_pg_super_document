# addRangeTableEntryForENR

## Location
src/backend/parser/parse_relation.c: 2466 - 2574

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
  - ENRMetadataGetTupDesc (to get tuple descriptor from ENR metadata)
  - [makeAlias](../m/makeAlias.md) (for alias creation)
  - [buildRelationAliases](../b/buildRelationAliases.md) (for column alias resolution)
  - lappend_oid, lappend_int (for column type management)
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