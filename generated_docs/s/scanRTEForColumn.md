# scanRTEForColumn

## Location
[src/backend/parser/parse_relation.c:800-882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L800-L882)

## Overview
Searches for a column name within a single Range Table Entry, returning the attribute number if found and optionally updating fuzzy match state for error reporting.

## Definition

```c
static int
scanRTEForColumn(ParseState *pstate, RangeTblEntry *rte,
				 Alias *eref,
				 const char *colname, int location,
				 int fuzzy_rte_penalty,
				 FuzzyAttrMatchState *fuzzystate)
```
## Detailed Description
This static function performs the core column name lookup within a specific Range Table Entry (RTE). It searches through the column names or aliases specified in the eref parameter, which can represent either all columns of a relation (via rte->eref) or just the common columns in a join (via rte->join_using_alias). The function handles both user-defined columns and system columns, with user aliases taking precedence over system column names. When fuzzy matching is enabled, it updates the fuzzy match state to help generate helpful error messages for misspelled column names. The function is designed to be minimal in validation checks to support error reporting scenarios where RTEs may not be in the active namespace.

## Parameters / Member Variables
- `*pstate`: ParseState pointer for error reporting context
- `*rte`: RangeTblEntry pointer representing the table/relation to search
- `*eref`: Alias pointer containing the column names to search (either rte->eref or rte->join_using_alias)
- `*colname`: String containing the column name to search for
- `location`: Integer representing parse location for error reporting
- `fuzzy_rte_penalty`: Integer penalty value for fuzzy matching calculations
- `*fuzzystate`: Pointer to FuzzyAttrMatchState for updating approximate match information (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [Alias](../A/Alias.md)
  - [FuzzyAttrMatchState](../F/FuzzyAttrMatchState.md)
  - InvalidAttrNumber
  - [updateFuzzyAttrMatchState](../u/updateFuzzyAttrMatchState.md)
  - RTE_RELATION
  - RELKIND_COMPOSITE_TYPE
  - [specialAttNum](specialAttNum.md)
  - SearchSysCacheExists2
  - [Int16GetDatum](../I/Int16GetDatum.md)
- Called from (representative examples):
  - MAX_FUZZY_DISTANCE
  - [scanNSItemForColumn](scanNSItemForColumn.md)
  - [searchRangeTableForCol](searchRangeTableForCol.md)

## Notes and Other Information
- Static function, only accessible within parse_relation.c
- Returns InvalidAttrNumber if no match found, positive attnum for user columns, negative for system columns
- Handles dropped columns safely by treating empty string column names as non-matches
- User column aliases override system column names without error
- System columns only considered for real relations, not composite types
- Supports fuzzy matching for improved error messages when exact matches fail
- Essential building block for PostgreSQL's column name resolution system
- Located in src/backend/parser/parse_relation.c:800-882

## Simplified Source

```c
static int
scanRTEForColumn(ParseState *pstate, RangeTblEntry *rte, Alias *eref,
                 const char *colname, int location,
                 int fuzzy_rte_penalty, FuzzyAttrMatchState *fuzzystate) {
    int result = InvalidAttrNumber;
    int attnum = 0;

    // Search through user column names for exact matches
    foreach(c, eref->colnames) {
        const char *attcolname = strVal(lfirst(c));
        attnum++;

        if (strcmp(attcolname, colname) == 0) {
            // Check for ambiguous column references
            if (result)
                ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                               errmsg("column reference \"%s\" is ambiguous", colname),
                               parser_errposition(pstate, location)));
            result = attnum;
        }

        // Update fuzzy match state for error reporting
        if (fuzzystate != NULL)
            updateFuzzyAttrMatchState(fuzzy_rte_penalty, fuzzystate,
                                     rte, attcolname, colname, attnum);
    }

    // Return user column match if found (overrides system columns)
    if (result)
        return result;

    // Check system columns for real relations (not composite types)
    if (rte->rtekind == RTE_RELATION && rte->relkind != RELKIND_COMPOSITE_TYPE) {
        attnum = specialAttNum(colname);
        if (attnum != InvalidAttrNumber) {
            // Verify the system column actually exists for this relation
            if (SearchSysCacheExists2(ATTNUM, ObjectIdGetDatum(rte->relid),
                                     Int16GetDatum(attnum)))
                result = attnum;
        }
    }

    return result;
}
```