# unknown_attribute

## Location
[src/backend/parser/parse_expr.c:392-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L392-L437)

## Overview
 is a specialized error reporting function that generates appropriate "column does not exist" error messages when field selection fails on an arbitrary node.

## Definition

```c
static void
unknown_attribute(ParseState *pstate, Node *relref, const char *attname,
				  int location)
```
## Detailed Description
 serves as a centralized error reporting mechanism for cases where column or attribute access fails during expression transformation. The function intelligently determines the appropriate error message based on the type and context of the expression being accessed. It distinguishes between range table entries (table/alias references) and arbitrary expression types, providing contextually relevant error messages. For range table entries, it reports missing columns with table/alias names. For complex types, it indicates the column doesn't exist in that data type. For record types, it provides a specific message about unidentifiable columns. For non-composite types, it explains that column notation cannot be applied to scalar types.

## Parameters / Member Variables
- : ParseState structure containing current parsing context for error reporting and position tracking
- : The node representing the object being accessed (could be a Var, expression, etc.)
- : The name of the attribute/column that could not be found
- : The location in the source query for precise error positioning

## Dependencies
- Functions called/Symbols referenced:
  - InvalidAttrNumber (constant for checking if this is a whole-row reference)
  - [GetRTEByRangeTablePosn](../G/GetRTEByRangeTablePosn.md) (retrieves range table entry information)
  - ISCOMPLEX (macro to check if a type is composite)
  - [exprType](../e/exprType.md), format_type_be (type system utilities)
  - ereport, errcode, errmsg, parser_errposition (error reporting system)

- Called from (representative examples):
  - [transformIndirection](../t/transformIndirection.md) (when field selection fails during indirection processing)

## Notes and Other Information  
- This is a static function, only accessible within the parse_expr.c module
- Provides three distinct error message patterns depending on the context:
  - Table/alias context: "column alias.name does not exist"
  - [Complex](../C/Complex.md) type context: "column 'name' not found in data type typename"  
  - Non-composite type context: "column notation .name applied to non-composite type"
- Uses different error codes (ERRCODE_UNDEFINED_COLUMN vs ERRCODE_WRONG_OBJECT_TYPE) to distinguish between missing columns and inappropriate usage
- Critical for providing user-friendly error messages during field access resolution
- Part of PostgreSQL's comprehensive error reporting system that provides precise source location information