# transformColumnDefinition

## Location
[src/backend/parser/parse_utilcmd.c:562-902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L562-L902)

## Overview
Transforms a single ColumnDef within CREATE TABLE or ALTER TABLE ADD COLUMN statements, processing column types, constraints, and special column types like SERIAL and IDENTITY.

## Definition

```c
static void
transformColumnDefinition(CreateStmtContext *cxt, ColumnDef *column)
```
## Detailed Description
transformColumnDefinition processes individual column definitions during table creation or alteration. It handles the complete transformation of column specifications including:

1. **SERIAL pseudo-type processing**: Converts SERIAL, BIGSERIAL, SMALLSERIAL types into their underlying integer types and generates associated sequence infrastructure
2. **Column type transformation**: Processes the column's data type specification through transformColumnType
3. **Constraint processing**: Validates and categorizes various column constraints (NOT NULL, DEFAULT, CHECK, PRIMARY KEY, UNIQUE, FOREIGN KEY, IDENTITY, GENERATED)
4. **Conflict detection**: Identifies conflicting constraint specifications (e.g., multiple defaults, conflicting NULL/NOT NULL declarations)
5. **Foreign data wrapper options**: Handles per-column options for foreign tables

For SERIAL columns, the function automatically creates NOT NULL and DEFAULT nextval() constraints. For IDENTITY columns, it generates the underlying sequence and sets up proper ownership relationships. The function also enforces business rules about which constraint combinations are valid and which are mutually exclusive.

## Parameters / Member Variables
- : CreateStmtContext containing parsing state and accumulating lists of various statement types
- : ColumnDef structure representing the column being processed

## Dependencies
- Functions called/Symbols referenced:
  - [transformColumnType](transformColumnType.md)
  - [generateSerialExtraStmts](../g/generateSerialExtraStmts.md)
  - [transformConstraintAttrs](transformConstraintAttrs.md)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md)
  - [typenameType](typenameType.md)
  - [makeFuncCall](../m/makeFuncCall.md)
  - makeNode (A_Const, TypeCast, Constraint, AlterTableStmt, AlterTableCmd)
  - [makeString](../m/makeString.md)
  - SystemTypeName
  - SystemFuncName
- Called from (representative examples):
  - [transformCreateStmt](transformCreateStmt.md)
  - [transformAlterTableStmt](transformAlterTableStmt.md)

## Notes and Other Information
The function maintains strict validation of constraint combinations, preventing conflicting specifications like both DEFAULT and IDENTITY on the same column. SERIAL types are treated as pseudo-types that expand into integer types with associated sequences and constraints. For identity columns, the function ensures they are implicitly NOT NULL and validates that they're not used on typed tables or partitions. Foreign table columns have restricted constraint support, prohibiting PRIMARY KEY, UNIQUE, and FOREIGN KEY constraints. The function accumulates constraints into different lists within the CreateStmtContext for later processing in the appropriate order.

## Simplified Source

```c
static void transformColumnDefinition(CreateStmtContext *cxt, ColumnDef *column) {
    bool is_serial = false;
    bool saw_nullable = false, saw_default = false;
    bool saw_identity = false, saw_generated = false;

    cxt->columns = lappend(cxt->columns, column);

    // Check for SERIAL pseudo-types
    if (column->typeName && list_length(column->typeName->names) == 1) {
        char *typname = strVal(linitial(column->typeName->names));

        if (strcmp(typname, "serial") == 0 || strcmp(typname, "serial4") == 0) {
            is_serial = true;
            column->typeName->typeOid = INT4OID;
        } else if (strcmp(typname, "bigserial") == 0 || strcmp(typname, "serial8") == 0) {
            is_serial = true;
            column->typeName->typeOid = INT8OID;
        } else if (strcmp(typname, "smallserial") == 0 || strcmp(typname, "serial2") == 0) {
            is_serial = true;
            column->typeName->typeOid = INT2OID;
        }
    }

    // Process column type
    if (column->typeName)
        transformColumnType(cxt, column);

    // Generate sequence for SERIAL columns
    if (is_serial) {
        generateSerialExtraStmts(cxt, column, column->typeName->typeOid, NIL,
                               false, false, &snamespace, &sname);

        // Add DEFAULT nextval() constraint
        Constraint *constraint = makeNode(Constraint);
        constraint->contype = CONSTR_DEFAULT;
        constraint->raw_expr = (Node *) makeFuncCall(SystemFuncName("nextval"), ...);
        column->constraints = lappend(column->constraints, constraint);

        // Add NOT NULL constraint
        constraint = makeNode(Constraint);
        constraint->contype = CONSTR_NOTNULL;
        column->constraints = lappend(column->constraints, constraint);
    }

    // Process column constraints
    transformConstraintAttrs(cxt, column->constraints);

    foreach(clist, column->constraints) {
        Constraint *constraint = lfirst_node(Constraint, clist);

        switch (constraint->contype) {
            case CONSTR_NULL:
                column->is_not_null = false;
                saw_nullable = true;
                break;

            case CONSTR_NOTNULL:
                column->is_not_null = true;
                saw_nullable = true;
                break;

            case CONSTR_DEFAULT:
                column->raw_default = constraint->raw_expr;
                saw_default = true;
                break;

            case CONSTR_IDENTITY:
                generateSerialExtraStmts(cxt, column, typeOid, constraint->options,
                                       true, false, NULL, NULL);
                column->identity = constraint->generated_when;
                column->is_not_null = true;
                saw_identity = saw_nullable = true;
                break;

            case CONSTR_GENERATED:
                column->generated = ATTRIBUTE_GENERATED_STORED;
                column->raw_default = constraint->raw_expr;
                saw_generated = true;
                break;

            case CONSTR_CHECK:
                cxt->ckconstraints = lappend(cxt->ckconstraints, constraint);
                break;

            case CONSTR_PRIMARY:
            case CONSTR_UNIQUE:
                constraint->keys = list_make1(makeString(column->colname));
                cxt->ixconstraints = lappend(cxt->ixconstraints, constraint);
                break;

            case CONSTR_FOREIGN:
                constraint->fk_attrs = list_make1(makeString(column->colname));
                cxt->fkconstraints = lappend(cxt->fkconstraints, constraint);
                break;
        }

        // Validate constraint combinations
        if (saw_default && saw_identity)
            ereport(ERROR, (errmsg("both default and identity specified")));
        if (saw_default && saw_generated)
            ereport(ERROR, (errmsg("both default and generation expression specified")));
        if (saw_identity && saw_generated)
            ereport(ERROR, (errmsg("both identity and generation expression specified")));
    }

    // Handle foreign data wrapper options
    if (column->fdwoptions != NIL) {
        // Generate ALTER FOREIGN TABLE statement for column options
        AlterTableStmt *stmt = makeNode(AlterTableStmt);
        // ... setup alter table command
        cxt->alist = lappend(cxt->alist, stmt);
    }
}
```