# expandTupleDesc

## Location
[src/backend/parser/parse_relation.c:3042-3122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3042-L3122)

## Overview
A specialized subroutine that generates column names and Var nodes for a specified range of attributes within a tuple descriptor, handling dropped columns and alias mappings appropriately.

## Definition

```c
static void
expandTupleDesc(TupleDesc tupdesc, Alias *eref, int count, int offset,
				int rtindex, int sublevels_up,
				int location, bool include_dropped,
				List **colnames, List **colvars)
```
## Detailed Description
This function serves as the core column expansion engine for PostgreSQL's parser, working directly with tuple descriptors to extract column information. It processes a specified range of attributes from a tuple descriptor and generates corresponding output lists based on the requested parameters.

Key functionality includes:

1. **Selective Processing**: Processes only the first 'count' attributes starting from position 'offset', allowing for partial tuple descriptor expansion
2. **Alias Handling**: Uses provided aliases from eref->colnames when available, falling back to underlying attribute names when aliases are exhausted
3. **Dropped Column Management**: Handles dropped columns by either including them as empty strings/NULL constants or skipping them entirely based on the include_dropped flag
4. **Var Node Creation**: Creates properly formatted Var nodes with correct varattno values (adjusted by offset), type information, and location data

The function is particularly important for handling composite-returning functions in RTE_FUNCTION contexts, where the offset and count parameters allow processing of individual function outputs within a larger function list.

## Parameters / Member Variables
- : Tuple descriptor containing attribute metadata to process
- : Alias information containing alternative column names, may have fewer entries than attributes
- : Number of attributes to process from the tuple descriptor (must be ≤ tupdesc->natts)
- : Starting position for attribute numbering in Var nodes and alias matching
- : Range table index for created Var nodes, identifying the relation in query context
- : Nesting level indicator for Var nodes in subquery contexts
- : Source location information for error reporting and debugging
- : Boolean flag controlling whether dropped columns are included (as empty strings/NULL) or omitted
- : Output parameter for list of column name strings (pass NULL if not needed)
- : Output parameter for list of Var nodes representing columns (pass NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (macro for accessing attribute information)
  - makeVar (creating Var nodes with proper type information)
  - [makeString](../m/makeString.md) (creating string nodes for column names)
  - [makeNullConst](../m/makeNullConst.md) (creating NULL constants for dropped columns)
  - list_nth_cell, lnext (list navigation functions)
  - [pstrdup](../p/pstrdup.md) (string duplication)
- Data structures used:
  - [TupleDesc](../T/TupleDesc.md), Form_pg_attribute (tuple descriptor structures)
  - [Alias](../A/Alias.md) (alias information)
  - Var (variable reference nodes)
- Called from:
  - [expandRelation](expandRelation.md) (for regular table relations)
  - [expandRTE](expandRTE.md) (for composite function returns)

## Notes and Other Information
- The function is static, indicating it's an internal helper within parse_relation.c
- Uses varattno + offset + 1 for Var node attribute numbers (PostgreSQL uses 1-based attribute numbering)
- Gracefully handles cases where there are fewer aliases than attributes by falling back to underlying names
- For dropped columns, uses INT4OID as a placeholder type since the actual type information may not be reliable
- The offset parameter enables this function to be used for individual functions within RTE_FUNCTION entries that return multiple result sets
- Critical for ensuring that column expansion preserves proper type information, collation, and type modifiers
- Forms the foundation for more complex expansion operations like expandRTE and expandRelation