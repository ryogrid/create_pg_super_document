# resolve_unique_index_expr

## Location
[src/backend/parser/parse_clause.c:3201-3296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L3201-L3296)

## Overview
Analyzes and transforms expressions and column references appearing in ON CONFLICT clauses to create a list of inference elements that will be used during planning to identify which unique index to use for conflict resolution.

## Definition

```c
structure for unique index
		 * inference clause, and so will accept opclasses by name and so on.
		 *
		 * Make no attempt to match ASC or DESC ordering or NULLS FIRST/NULLS
		 * LAST ordering, since those are not significant for inference
		 * purposes (any unique index matching the inference specification in
		 * other regards is accepted indifferently).  Actively reject this as
		 * wrong-headed.
		 */
		if (ielem->ordering != SORTBY_DEFAULT)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("ASC/DESC is not allowed in ON CONFLICT clause"),
					 parser_errposition(pstate,
										exprLocation((Node *) infer))));
```
## Detailed Description
This static function is a critical component of PostgreSQL's ON CONFLICT (UPSERT) functionality. It processes the index elements specified in an ON CONFLICT clause, transforming them from their raw parsed form into InferenceElem structures that the planner can use to match against actual unique indexes on the target table.

The function performs several important tasks: it rejects invalid syntax like ASC/DESC and NULLS FIRST/LAST ordering (which are meaningless for conflict detection), transforms column references and expressions using the standard expression transformation infrastructure, and resolves collation and operator class specifications. For simple column references, it constructs ColumnRef nodes since the grammar doesn't build raw expressions for plain column names.

The resulting list of InferenceElem structures contains the transformed expressions along with their associated collation and operator class information, providing all the information needed by the planner to match against available unique indexes on the target relation.

## Parameters / Member Variables
- : Parse state context containing parsing information and error handling state
- : InferClause containing the raw index elements from the ON CONFLICT clause
- : The target relation (table) for the INSERT statement with ON CONFLICT

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Creates a new PostgreSQL node structure (for InferenceElem and ColumnRef)
  - [makeString](../m/makeString.md): Creates a string value node
  - [transformExpr](../t/transformExpr.md): Transforms a raw expression using standard expression transformation
  - [LookupCollation](../L/LookupCollation.md): Resolves collation names to OIDs
  - [get_opclass_oid](../g/get_opclass_oid.md): Resolves operator class names to OIDs for B-tree access method
  - [exprLocation](../e/exprLocation.md): Gets parse location of expressions for error reporting
  - InferClause, IndexElem, InferenceElem, ColumnRef: Node structures for inference processing
  - EXPR_KIND_INDEX_EXPRESSION: Expression kind constant that restricts allowed expression types
  - SORTBY_DEFAULT, SORTBY_NULLS_DEFAULT: Constants for default sort ordering
- Called from (representative examples):
  - [transformOnConflictArbiter](../t/transformOnConflictArbiter.md): Main function that processes ON CONFLICT arbiter specifications

## Notes and Other Information
- This is a static function, only used within parse_clause.c for ON CONFLICT processing
- Actively rejects ASC/DESC and NULLS FIRST/LAST specifications as they are not significant for unique index inference
- Reuses CREATE INDEX infrastructure for parsing but restricts functionality to what's meaningful for conflict resolution
- The expression transformation uses EXPR_KIND_INDEX_EXPRESSION which automatically rejects subqueries, aggregates, window functions, and set-returning functions
- For simple column references, manually constructs ColumnRef nodes since the grammar doesn't create raw expressions for plain column names
- Sets location information for error reporting to approximately match the inference specification location
- Collation and operator class resolution uses InvalidOid when not specified, allowing the system to use defaults

## Simplified Source

```c
static List *
resolve_unique_index_expr(ParseState *pstate, InferClause *infer, Relation heapRel)
{
    List *result = NIL;
    ListCell *l;

    foreach(l, infer->indexElems) {
        IndexElem *ielem = (IndexElem *) lfirst(l);
        InferenceElem *pInfer = makeNode(InferenceElem);
        Node *parse;

        // Reject ordering specifications (not meaningful for conflict detection)
        if (ielem->ordering != SORTBY_DEFAULT)
            ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("ASC/DESC is not allowed in ON CONFLICT clause")));

        if (ielem->nulls_ordering != SORTBY_NULLS_DEFAULT)
            ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("NULLS FIRST/LAST is not allowed in ON CONFLICT clause")));

        if (!ielem->expr) {
            // Simple column reference - create ColumnRef node
            ColumnRef *n = makeNode(ColumnRef);
            n->fields = list_make1(makeString(ielem->name));
            n->location = infer->location;
            parse = (Node *) n;
        } else {
            // Complex expression
            parse = (Node *) ielem->expr;
        }

        // Transform expression (rejects subqueries, aggregates, etc.)
        pInfer->expr = transformExpr(pstate, parse, EXPR_KIND_INDEX_EXPRESSION);

        // Resolve collation and operator class
        if (!ielem->collation)
            pInfer->infercollid = InvalidOid;
        else
            pInfer->infercollid = LookupCollation(pstate, ielem->collation,
                                                 exprLocation(pInfer->expr));

        if (!ielem->opclass)
            pInfer->inferopclass = InvalidOid;
        else
            pInfer->inferopclass = get_opclass_oid(BTREE_AM_OID,
                                                  ielem->opclass, false);

        result = lappend(result, pInfer);
    }

    return result;
}
```