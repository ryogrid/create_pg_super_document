# transformJsonArrayQueryConstructor

## Location
[src/backend/parser/parse_expr.c:3751-3821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3751-L3821)

## Overview
Transforms JSON_ARRAY(query) constructor expressions into equivalent JSON_ARRAYAGG subquery expressions for PostgreSQL's JSON array construction from query results.

## Definition

```c
static Node *
transformJsonArrayQueryConstructor(ParseState *pstate,
								   JsonArrayQueryConstructor *ctor)
```
## Detailed Description
This function transforms JSON_ARRAY(query [FORMAT] [RETURNING] [ON NULL]) syntax into an equivalent subquery expression using JSON_ARRAYAGG. The transformation converts the original query-based JSON array constructor into:


The transformation process involves several steps:
1. Validating that the input query returns exactly one column
2. Creating a subquery structure with proper aliasing (table alias 'q', column alias 'a')
3. Constructing a JsonArrayAgg node that aggregates the query results
4. Building a complete SELECT statement with the aggregation as the target
5. Wrapping everything in a SubLink expression for execution

The function ensures proper error handling for queries that return multiple columns, as JSON_ARRAY can only construct arrays from single-column results.

## Parameters / Member Variables
- `*pstate`: ParseState pointer containing current parsing context and state information for the transformation
- `*ctor`: JsonArrayQueryConstructor pointer containing the source JSON_ARRAY(query) constructor expression to be transformed
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating various AST nodes: SubLink, SelectStmt, RangeSubselect, Alias, ResTarget, JsonArrayAgg, ColumnRef, JsonAggConstructor)
  - [make_parsestate](../m/make_parsestate.md)/free_parsestate (for temporary parsing context management)
  - [transformStmt](transformStmt.md) (for transforming the inner query)
  - copyObject (for creating a copy of the query for validation)
  - [count_nonjunk_tlist_entries](../c/count_nonjunk_tlist_entries.md) (for validating single-column requirement)
  - [makeJsonValueExpr](../m/makeJsonValueExpr.md) (for creating JSON value expressions)
  - list_make1/list_make2 (for creating lists)
  - [makeString](../m/makeString.md)/pstrdup (for string manipulation)
  - [transformExprRecurse](transformExprRecurse.md) (for final expression transformation)
  - EXPR_SUBLINK (sublink type constant)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- The function enforces that the input query must return exactly one column, raising a syntax error otherwise
- The generated subquery uses fixed aliases: 'q' for the table and 'a' for the column
- Format specifications and null handling options are preserved from the original constructor
- The transformation creates a complete executable subquery that can be processed by the query executor
- All location information is preserved for accurate error reporting
- The function handles both formatted and unformatted JSON value expressions appropriately

## Simplified Source

```c
static Node *
transformJsonArrayQueryConstructor(ParseState *pstate,
                                   JsonArrayQueryConstructor *ctor)
{
    // Create nodes for the transformed subquery structure
    SubLink *sublink = makeNode(SubLink);
    SelectStmt *select = makeNode(SelectStmt);
    RangeSubselect *range = makeNode(RangeSubselect);
    Alias *alias = makeNode(Alias);
    ResTarget *target = makeNode(ResTarget);
    JsonArrayAgg *agg = makeNode(JsonArrayAgg);
    ColumnRef *colref = makeNode(ColumnRef);

    // Validate that the query returns exactly one column
    ParseState *qpstate = make_parsestate(pstate);
    Query *query = transformStmt(qpstate, copyObject(ctor->query));

    if (count_nonjunk_tlist_entries(query->targetList) != 1)
        ereport(ERROR,
                errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("subquery must return only one column"),
                parser_errposition(pstate, ctor->location));

    free_parsestate(qpstate);

    // Create column reference q.a for the aggregation
    colref->fields = list_make2(makeString(pstrdup("q")),
                                makeString(pstrdup("a")));
    colref->location = ctor->location;

    // Build JsonArrayAgg expression
    agg->arg = makeJsonValueExpr((Expr *) colref, (Expr *) colref, ctor->format);
    agg->absent_on_null = ctor->absent_on_null;
    agg->constructor = makeNode(JsonAggConstructor);
    agg->constructor->agg_order = NIL;
    agg->constructor->output = ctor->output;
    agg->constructor->location = ctor->location;

    // Build SELECT target
    target->name = NULL;
    target->indirection = NIL;
    target->val = (Node *) agg;
    target->location = ctor->location;

    // Set up subquery alias (table 'q', column 'a')
    alias->aliasname = pstrdup("q");
    alias->colnames = list_make1(makeString(pstrdup("a")));

    // Create range subselect
    range->lateral = false;
    range->subquery = ctor->query;
    range->alias = alias;

    // Build the SELECT statement
    select->targetList = list_make1(target);
    select->fromClause = list_make1(range);

    // Wrap in SubLink
    sublink->subLinkType = EXPR_SUBLINK;
    sublink->subLinkId = 0;
    sublink->testexpr = NULL;
    sublink->operName = NIL;
    sublink->subselect = (Node *) select;
    sublink->location = ctor->location;

    return transformExprRecurse(pstate, (Node *) sublink);
}
```