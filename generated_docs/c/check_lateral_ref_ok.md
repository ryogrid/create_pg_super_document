# check_lateral_ref_ok

## Location
src/backend/parser/parse_relation.c: 483 - 509

## Overview
Validates that a namespace item is not currently disallowed as a LATERAL reference and raises an appropriate error if it is.

## Definition
```c
static void check_lateral_ref_ok(ParseState *pstate, ParseNamespaceItem *nsitem, int location)
```

## Detailed Description
This function enforces both SQL:2008 standard rules for LATERAL references and PostgreSQL's own restrictions. It checks whether a namespace item that is marked as lateral-only (`p_lateral_only`) but not currently allowed (`p_lateral_ok` is false) is being referenced inappropriately. This situation can occur in two main scenarios:

1. **SQL:2008 compliance**: LATERAL reference to the wrong side of an outer join (must be INNER or LEFT JOIN for LATERAL references)
2. **PostgreSQL-specific rule**: Prohibition on referencing the target table of an UPDATE or DELETE statement as a lateral reference in a FROM/USING clause

The function provides context-aware error messages, giving different hints depending on whether the problematic reference is to the target table of the current statement or involves improper JOIN types.

## Parameters / Member Variables
- `pstate`: Pointer to the ParseState structure for the current query level where the nsitem was found
- `nsitem`: Pointer to the ParseNamespaceItem being checked for lateral reference validity
- `location`: Parser location information for error positioning in the query text

## Dependencies
- Functions called/Symbols referenced:
  - ereport/ERROR (for error reporting)
  - [parser_errposition](../p/parser_errposition.md) (for error location reporting)
- Types referenced:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [RangeTblEntry](../R/RangeTblEntry.md)
- Called from (representative examples):
  - [scanNameSpaceForRefname](../s/scanNameSpaceForRefname.md) (src/backend/parser/parse_relation.c:224)
  - [scanNameSpaceForRelid](../s/scanNameSpaceForRelid.md) (src/backend/parser/parse_relation.c:268)
  - [colNameToVar](colNameToVar.md) (src/backend/parser/parse_relation.c:918)

## Notes and Other Information
- This is a static convenience function to avoid code duplication of error reporting logic
- Implements SQL:2008 standard requirements for LATERAL references
- Provides context-sensitive error messages with appropriate hints
- Only triggers when both `p_lateral_only` is true AND `p_lateral_ok` is false
- Uses ERRCODE_INVALID_COLUMN_REFERENCE error code
- The function name suggests it checks if lateral references are "ok", but it actually enforces restrictions
- Used throughout the parser when resolving references that might be lateral-only