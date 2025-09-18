# buildVarFromNSColumn

## Location
src/backend/parser/parse_clause.c: 1640 - 1665

## Overview
Constructs a Var node from ParseNamespaceColumn data, primarily used for building join alias variables.

## Definition
```c
static Var *buildVarFromNSColumn(ParseState *pstate, ParseNamespaceColumn *nscol)
```

## Detailed Description
This function serves as a constructor for Var nodes based on ParseNamespaceColumn information, specifically designed for creating join alias variables. It creates a Var node using makeVar with the column's type information (varno, varattno, type, typmod, collation) and sets varlevelsup to 0, indicating it's a base-level reference. The function then manually sets the syntactic reference information (varnosyn and varattnosyn) which makeVar doesn't support as parameters. Finally, it calls markNullableIfNeeded to update the varnullingrels bitmask based on the current parse state's nulling context, ensuring the Var correctly reflects any outer join nullability effects. This function is essential for maintaining proper variable semantics in complex join operations where columns need to be accessible through join aliases.

## Parameters / Member Variables
- `pstate`: ParseState containing current parsing context and nulling information
- `nscol`: ParseNamespaceColumn containing the column metadata to construct the Var from

## Dependencies
- Functions called/Symbols referenced:
  - makeVar
  - markNullableIfNeeded
- Types referenced:
  - ParseNamespaceColumn
  - Var
- Called from (representative examples):
  - transformFromClauseItem (for USING clause processing)
  - extractRemainingColumns (for column extraction in joins)

## Notes and Other Information
- This is a static function within parse_clause.c used internally for FROM clause processing
- Specifically designed for join alias variable construction, not general column references
- Does not request column SELECT privileges since these are internal alias variables
- Always sets varlevelsup to 0 as these are base-level references
- The function ensures proper nulling semantics by calling markNullableIfNeeded
- Critical for maintaining correct variable semantics in JOIN operations with aliases