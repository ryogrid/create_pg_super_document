# colNameToVar

## Location
[src/backend/parser/parse_relation.c:883-951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L883-L951)

## Overview
Searches for an unqualified column name in the parser namespace and returns the appropriate Var node or expression if found.

## Definition


## Detailed Description
The  function performs an unqualified column name lookup within the PostgreSQL parser. It searches through the parser namespace hierarchy, starting from the current parse state and potentially traversing up to parent parse states. The function handles ambiguity detection by raising an error if the same column name is found in multiple namespace items. It also respects lateral reference rules and visibility constraints for namespace items.

The search process iterates through all namespace items in the current parse state, filtering out items that are not column-visible or lateral-only items when not in a lateral context. For each valid namespace item, it calls  to perform the actual column search.

## Parameters / Member Variables
- : The current parse state containing the namespace to search
- : The unqualified column name to search for
- : If true, only search in the innermost query level (don't traverse parent parse states)
- : Source location for error reporting purposes

## Dependencies
- Functions called/Symbols referenced:
  - [scanNSItemForColumn](../s/scanNSItemForColumn.md)
  - [check_lateral_ref_ok](check_lateral_ref_ok.md)
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [FuzzyAttrMatchState](../F/FuzzyAttrMatchState.md)
- Called from (representative examples):
  - [findTargetlistEntrySQL92](../f/findTargetlistEntrySQL92.md)
  - [CRERR_TOO_MANY](../C/CRERR_TOO_MANY.md)

## Notes and Other Information
- Returns NULL if the column name is not found
- Raises an ERROR with ERRCODE_AMBIGUOUS_COLUMN if the column name is ambiguous
- Handles lateral reference validation through check_lateral_ref_ok
- The function maintains consistency by using the original parse state for scanNSItemForColumn calls
- Supports hierarchical namespace searching unless localonly is specified