# transformAssignedExpr

## Location
[src/backend/parser/parse_target.c:452-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L452-L618)

## Overview
Prepares an expression for assignment to a column in INSERT and UPDATE statements, handling type coercion and processing any subfield names or subscripts attached to the target column.

## Definition

```c
structed by INSERT or UPDATE.
	 */
	if (indirection)
	{
		Node	   *colVar;

		if (pstate->p_is_insert)
		{
			/*
			 * The command is INSERT INTO table (col.something) ... so there
			 * is not really a source value to work with. Insert a NULL
			 * constant as the source value.
			 */
			colVar = (Node *) makeNullConst(attrtype, attrtypmod,
											attrcollation);
		}
		else
		{
			/*
			 * Build a Var for the column to be updated.
			 */
			Var		   *var;

			var = makeVar(pstate->p_target_nsitem->p_rtindex, attrno,
						  attrtype, attrtypmod, attrcollation, 0);
			var->location = location;

			colVar = (Node *) var;
		}

		expr = (Expr *)
			transformAssignmentIndirection(pstate,
										   colVar,
										   colname,
										   false,
										   attrtype,
										   attrtypmod,
										   attrcollation,
										   indirection,
										   list_head(indirection),
										   (Node *) expr,
										   COERCION_ASSIGNMENT,
										   location);
	}
	else
	{
		/*
		 * For normal non-qualified target column, do type checking and
		 * coercion.
		 */
		Node	   *orig_expr = (Node *) expr;

		expr = (Expr *)
			coerce_to_target_type(pstate,
								  orig_expr, type_id,
								  attrtype, attrtypmod,
								  COERCION_ASSIGNMENT,
								  COERCE_IMPLICIT_CAST,
								  -1);
		if (expr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("column \"%s\" is of type %s"
							" but expression is of type %s",
							colname,
							format_type_be(attrtype),
							format_type_be(type_id)),
					 errhint("You will need to rewrite or cast the expression."),
					 parser_errposition(pstate, exprLocation(orig_expr))));
	}

	pstate->p_expr_kind = sv_expr_kind;
```
## Detailed Description
This function is specifically used in INSERT and UPDATE statements to transform expressions before assignment to table columns. It performs several critical operations:

1. **Type Coercion**: Coerces the given expression to match the target column's type when necessary
2. **DEFAULT Handling**: Processes DEFAULT placeholders by inserting the target column's type information and validates that DEFAULT cannot be used with subfields or array elements
3. **Indirection Processing**: Handles subfield assignments and array subscript assignments by calling transformAssignmentIndirection
4. **System Column Protection**: Prevents assignments to system columns (attrno <= 0)

For indirection cases, the function distinguishes between INSERT and UPDATE operations:
- INSERT with indirection: creates a NULL constant as the base value since there's no existing column value
- UPDATE with indirection: creates a Var node representing the current column value

## Parameters
- `pstate`: Parse state containing context for the current query parsing
- `expr`: Expression to be transformed for assignment (already processed by transformExpr)
- `exprKind`: ParseExprKind indicating the type of statement context (INSERT vs UPDATE)
- `colname`: Name of the target column being assigned to
- `attrno`: Attribute number of the target column in the relation
- `indirection`: List of subscripts or field names for complex assignments (may be NULL)
- `location`: Error cursor position for the target column (-1 if not applicable)

## Dependencies
- Functions called/Symbols referenced:
  - [attnumTypeId](../a/attnumTypeId.md)
  - TupleDescAttr
  - IsA
  - [makeNullConst](../m/makeNullConst.md)
  - makeVar
  - [transformAssignmentIndirection](transformAssignmentIndirection.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - exprType
  - [exprLocation](../e/exprLocation.md)
  - [format_type_be](../f/format_type_be.md)
  - list_head
  - Constants: EXPR_KIND_NONE, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST
- Called from:
  - [transformInsertRow](transformInsertRow.md) (analyze.c)
  - [updateTargetListEntry](../u/updateTargetListEntry.md) (parse_target.c)

## Notes and Other Information
- The function temporarily modifies pstate->p_expr_kind during processing and restores it before returning
- Location parameter points at the target column name and should be -1 for INSERT statements that omit column name lists
- For error reporting in default INSERTs, exprLocation(expr) is preferred over the location parameter
- System columns cannot be assigned to, triggering an error if attempted
- DEFAULT expressions cannot be used for partial column updates (subfields or array elements)
- The function is essential for implementing PostgreSQL's assignment semantics with proper type safety