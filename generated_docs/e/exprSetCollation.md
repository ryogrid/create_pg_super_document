# exprSetCollation

## Location
[src/backend/nodes/nodeFuncs.c:1116-1315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L1116-L1315)

## Overview
Assigns collation information to an expression tree node during parse analysis, handling all expression node types and setting their appropriate collation fields.

## Definition

```c
structorExpr:
			{
				JsonConstructorExpr *ctor = (JsonConstructorExpr *) expr;

				if (ctor->coercion)
					exprSetCollation((Node *) ctor->coercion, collation);
				else
					Assert(!OidIsValid(collation)); /* result is always a
													 * json[b] type */
			}
			break;
```
## Detailed Description
The  function is responsible for assigning collation information to various types of expression tree nodes during PostgreSQL's parse analysis phase. It uses a comprehensive switch statement based on the node's tag to determine the appropriate collation field to set for each expression type.

The function handles over 30 different expression node types, including variables, constants, function calls, operators, and complex expressions like CASE and array expressions. For certain expression types that always produce non-collatable results (like boolean expressions), the function includes assertions to ensure no collation is being set.

This function is critical for PostgreSQL's collation support system, ensuring that string operations and comparisons use the correct collation rules throughout the query execution process.

## Parameters
- : The expression tree node to assign collation to
- : The OID of the collation to assign

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine expression type)
  - [exprCollation](exprCollation.md) (for validation in some cases)
  - linitial_node (for sublink processing)
  - Various expression node type constants (T_Var, T_Const, etc.)

- Called from:
  - [assign_collations_walker](../a/assign_collations_walker.md) (src/backend/parser/parse_collate.c:439, 441, 747, 749)
  - Recursively calls itself for nested expressions (JsonValueExpr, JsonConstructorExpr, JsonBehavior)

## Notes and Other Information
- Only used during parse analysis phase, so it doesn't need to handle subplans or PlaceHolderVars
- Includes extensive assertion checking to validate that collations are only set for collatable expression types
- Some expression types (BoolExpr, ScalarArrayOpExpr, etc.) assert that no collation should be set since they always return boolean results
- Handles recursive collation setting for complex JSON expressions
- Located in src/backend/nodes/nodeFuncs.c:1116-1315

## Simplified Source

```c
void
exprSetCollation(Node *expr, Oid collation)
{
    switch (nodeTag(expr))
    {
        // Basic expression types with collation fields
        case T_Var:
            ((Var *) expr)->varcollid = collation;
            break;
        case T_Const:
            ((Const *) expr)->constcollid = collation;
            break;
        case T_Param:
            ((Param *) expr)->paramcollid = collation;
            break;
        case T_Aggref:
            ((Aggref *) expr)->aggcollid = collation;
            break;
        case T_WindowFunc:
            ((WindowFunc *) expr)->wincollid = collation;
            break;
        case T_FuncExpr:
            ((FuncExpr *) expr)->funccollid = collation;
            break;
        case T_OpExpr:
            ((OpExpr *) expr)->opcollid = collation;
            break;
        case T_CaseExpr:
            ((CaseExpr *) expr)->casecollid = collation;
            break;
        case T_ArrayExpr:
            ((ArrayExpr *) expr)->array_collid = collation;
            break;
        case T_CoalesceExpr:
            ((CoalesceExpr *) expr)->coalescecollid = collation;
            break;
        case T_MinMaxExpr:
            ((MinMaxExpr *) expr)->minmaxcollid = collation;
            break;

        // Type coercion expressions
        case T_RelabelType:
            ((RelabelType *) expr)->resultcollid = collation;
            break;
        case T_CoerceViaIO:
            ((CoerceViaIO *) expr)->resultcollid = collation;
            break;
        case T_CoerceToDomain:
            ((CoerceToDomain *) expr)->resultcollid = collation;
            break;

        // Boolean result types - assert no collation
        case T_BoolExpr:
        case T_NullTest:
        case T_BooleanTest:
            Assert(!OidIsValid(collation));
            break;

        // JSON expressions with recursive handling
        case T_JsonValueExpr:
            exprSetCollation((Node *) ((JsonValueExpr *) expr)->formatted_expr, collation);
            break;
        case T_JsonExpr:
            ((JsonExpr *) expr)->collation = collation;
            break;

        // Additional expression types...
        // [Other cases follow similar pattern]

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
            break;
    }
}
```