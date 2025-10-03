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
- `*pstate`: ParseState structure containing current parsing context for error reporting and position tracking
- `*relref`: The node representing the object being accessed (could be a Var, expression, etc.)
- `*attname`: The name of the attribute/column that could not be found
- `location`: The location in the source query for precise error positioning
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

## Simplified Source

```c
static void
unknown_attribute(ParseState *pstate, Node *relref, const char *attname,
                  int location) {
    RangeTblEntry *rte;

    // Check if this is a table/alias reference
    if (IsA(relref, Var) && ((Var *) relref)->varattno == InvalidAttrNumber) {
        // Get the range table entry for error context
        rte = GetRTEByRangeTablePosn(pstate,
                                    ((Var *) relref)->varno,
                                    ((Var *) relref)->varlevelsup);

        // Report with table/alias name
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                 errmsg("column %s.%s does not exist",
                        rte->eref->aliasname, attname),
                 parser_errposition(pstate, location)));
    } else {
        // Handle arbitrary expression types
        Oid relTypeId = exprType(relref);

        if (ISCOMPLEX(relTypeId)) {
            // Complex type missing column
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                     errmsg("column \"%s\" not found in data type %s",
                            attname, format_type_be(relTypeId)),
                     parser_errposition(pstate, location)));
        } else if (relTypeId == RECORDOID) {
            // Record type with unidentifiable column
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                     errmsg("could not identify column \"%s\" in record data type",
                            attname),
                     parser_errposition(pstate, location)));
        } else {
            // Non-composite type accessed with column notation
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("column notation .%s applied to type %s, "
                            "which is not a composite type",
                            attname, format_type_be(relTypeId)),
                     parser_errposition(pstate, location)));
        }
    }
}
```