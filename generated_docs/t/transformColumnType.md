# transformColumnType

## Location
[src/backend/parser/parse_utilcmd.c:3752-3808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L3752-L3808)

## Overview
Validates and processes column type definitions during table creation or alteration, ensuring type validity and proper collation handling.

## Definition
static void transformColumnType(CreateStmtContext *cxt, ColumnDef *column)

## Detailed Description
The transformColumnType function performs specialized validation and processing of column type definitions within the context of CREATE TABLE or ALTER TABLE operations. Its key responsibilities include:

1. **Type Validation**: Verifies that the specified type name is valid and resolvable within the current database context using typenameType, which performs comprehensive type lookup and validation.

2. **Collation Processing**: When a collation clause is present:
   - Validates that the specified collation exists and is accessible via LookupCollation
   - Retrieves the type's system catalog information to check collation compatibility
   - Ensures that collations are only applied to types that support collation (text-like types)
   - Provides precise error reporting with location information when collation is inappropriately applied

3. **Error Reporting**: Generates detailed error messages when:
   - An unsupported type is specified
   - A collation is applied to a non-collatable type
   - The collation specification is invalid or inaccessible

4. **Resource Management**: Properly releases system cache entries obtained during type lookup to prevent memory leaks.

The function is designed to be called during the early phases of statement transformation to catch type and collation errors before proceeding with more complex processing steps.

## Parameters / Member Variables
- : CreateStmtContext structure containing the parsing state and context information for the table creation/alteration operation
- : ColumnDef structure representing the column definition whose type needs to be validated and processed

## Dependencies
- Functions called/Symbols referenced:
  - [typenameType](typenameType.md)
  - [LookupCollation](../L/LookupCollation.md)
  - OidIsValid
  - [format_type_be](../f/format_type_be.md)
  - [parser_errposition](../p/parser_errposition.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - CreateSchemaStmtContext
  - [transformColumnDefinition](transformColumnDefinition.md)

## Notes and Other Information
- This is a static function used internally within the parse_utilcmd.c module
- Performs early validation to prevent invalid type/collation combinations from proceeding further in the transformation process
- The function focuses specifically on type and collation validation, not other aspects of column definition
- Proper error location reporting helps users identify exactly where type/collation issues occur in their SQL
- Essential for maintaining type system integrity during DDL operations
- The collation validation prevents runtime errors by catching incompatible type/collation combinations at parse time
- Memory management through ReleaseSysCache is crucial for avoiding cache entry leaks during type validation

## Simplified Source

```c
static void transformColumnType(CreateStmtContext *cxt, ColumnDef *column) {
    // Validate that the specified type exists and is accessible
    Type type_info = typenameType(cxt->pstate, column->typeName, NULL);

    // If a collation clause is present, validate it
    if (column->collClause) {
        Form_pg_type type_form = (Form_pg_type) GETSTRUCT(type_info);

        // Check that the collation exists
        LookupCollation(cxt->pstate,
                       column->collClause->collname,
                       column->collClause->location);

        // Verify the type supports collation
        if (!OidIsValid(type_form->typcollation))
            ereport(ERROR,
                   (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("collations are not supported by type %s",
                           format_type_be(type_form->oid)),
                    parser_errposition(cxt->pstate,
                                     column->collClause->location)));
    }

    // Release the type cache entry
    ReleaseSysCache(type_info);
}
```