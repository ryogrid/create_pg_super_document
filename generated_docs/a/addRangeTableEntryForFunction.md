# addRangeTableEntryForFunction

## Location
src/backend/parser/parse_relation.c: 1734 - 2048

## Overview
Creates a range table entry for one or more functions in a FROM clause, handling complex type resolution, column definition validation, and tuple descriptor construction for function return types.

## Definition


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
  - makeAlias (for alias creation)
  - get_expr_result_type (function type analysis)
  - CreateTemplateTupleDesc (tuple descriptor creation)
  - TupleDescInitEntry, TupleDescInitEntryCollation (attribute initialization)
  - TupleDescCopyEntry (for merging multiple function results)
  - chooseScalarFunctionAlias (scalar function naming)
  - typenameTypeIdAndMod (type resolution from column definitions)
  - CheckAttributeNamesTypes (column validation)
  - buildRelationAliases (alias processing)
  - Various list manipulation functions (lappend, lappend_oid, lappend_int)
- Called from (representative examples):
  - transformRangeFunction (in parse_clause.c)

## Notes and Other Information
- Functions are never checked for access rights by the permission system since they represent computed results
- The function supports PostgreSQL's SETOF functions and handles ordinality columns for row numbering
- Complex error handling provides specific messages for different column definition list validation failures
- When multiple functions are present, their tuple descriptors are merged into a single composite descriptor
- The function name list is used primarily for error reporting and automatic alias generation
- LATERAL functions have special scoping rules allowing them to reference columns from preceding FROM items
- Column definition lists have strict limits to prevent exceeding PostgreSQL's tuple attribute limits
- Type resolution handles pseudo-types like RECORD with special validation rules