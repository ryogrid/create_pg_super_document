# transformJsonAggConstructor

## Location
[src/backend/parser/parse_expr.c:3822-3907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3822-L3907)

## Overview
Common transformation function for both JSON_OBJECTAGG and JSON_ARRAYAGG constructors, handling the creation of aggregate expressions and window functions for JSON aggregation operations.

## Definition

```c
static Node *
transformJsonAggConstructor(ParseState *pstate, JsonAggConstructor *agg_ctor,
							JsonReturning *returning, List *args,
							Oid aggfnoid, Oid aggtype,
							JsonConstructorType ctor_type,
							bool unique, bool absent_on_null)
```
## Detailed Description
This is a shared transformation function that handles the common functionality for both JSON_OBJECTAGG and JSON_ARRAYAGG expressions. The function determines whether to create a regular aggregate (Aggref) or a window function (WindowFunc) based on the presence of an OVER clause in the constructor.

The transformation process includes:
1. Processing any FILTER clause by transforming it into an aggregate filter expression
2. Determining whether to create a window function or regular aggregate based on the presence of an OVER clause
3. For window functions: creating a WindowFunc node with proper function ID, type, and arguments, while enforcing restrictions (no ORDER BY in window context)
4. For regular aggregates: creating an Aggref node and processing it through the standard aggregate transformation pipeline
5. Wrapping the final result in a JsonConstructorExpr with the appropriate constructor type and options

The function centralizes the common logic between JSON_OBJECTAGG and JSON_ARRAYAGG, ensuring consistent handling of filters, window functions, and aggregate transformations.

## Parameters / Member Variables
- : ParseState pointer containing current parsing context and state information
- : JsonAggConstructor pointer containing the aggregate constructor specification with ordering, filtering, and window information
- : JsonReturning pointer specifying the return type and formatting options for the JSON result
- : List pointer containing the argument expressions for the aggregate function
- : Oid specifying the object identifier of the aggregate function to be called
- : Oid specifying the return type of the aggregate function
- : JsonConstructorType enum indicating whether this is for JSON_OBJECTAGG or JSON_ARRAYAGG
- : boolean flag indicating whether unique key constraints should be enforced (relevant for JSON_OBJECTAGG)
- : boolean flag specifying the null handling behavior for the JSON constructor

## Dependencies
- Functions called/Symbols referenced:
  - [transformWhereClause](transformWhereClause.md) (for processing FILTER clauses)
  - makeNode (for creating WindowFunc and Aggref nodes)
  - [transformWindowFuncCall](transformWindowFuncCall.md) (for window function processing)
  - [transformAggregateCall](transformAggregateCall.md) (for standard aggregate processing)
  - [makeJsonConstructorExpr](../m/makeJsonConstructorExpr.md) (for creating the final JSON constructor expression)
  - EXPR_KIND_FILTER (expression kind constant for filters)
  - AGGKIND_NORMAL, AGGSPLIT_SIMPLE (aggregate processing constants)
- Called from (representative examples):
  - [transformJsonObjectAgg](transformJsonObjectAgg.md) (for JSON_OBJECTAGG transformation)
  - [transformJsonArrayAgg](transformJsonArrayAgg.md) (for JSON_ARRAYAGG transformation)

## Notes and Other Information
- The function enforces that ORDER BY clauses are not supported in window function contexts for JSON aggregates
- Window functions and regular aggregates follow different code paths but share common initialization logic
- Collation information (wincollid, inputcollid, aggcollid) is set later by parse_collate.c
- Aggregate-specific fields (aggtranstype, aggargtypes, etc.) are set by subsequent processing functions
- The function properly handles both filtered and unfiltered aggregates
- All location information is preserved for accurate error reporting throughout the transformation process

## Simplified Source

```c
static Node *
transformJsonAggConstructor(ParseState *pstate, JsonAggConstructor *agg_ctor,
                           JsonReturning *returning, List *args,
                           Oid aggfnoid, Oid aggtype,
                           JsonConstructorType ctor_type,
                           bool unique, bool absent_on_null) {
    Node *node;
    Expr *aggfilter;

    // Transform any FILTER clause
    aggfilter = agg_ctor->agg_filter
        ? (Expr *) transformWhereClause(pstate, agg_ctor->agg_filter,
                                       EXPR_KIND_FILTER, "FILTER")
        : NULL;

    if (agg_ctor->over) {
        // Create window function
        WindowFunc *wfunc = makeNode(WindowFunc);

        wfunc->winfnoid = aggfnoid;
        wfunc->wintype = aggtype;
        wfunc->args = args;
        wfunc->aggfilter = aggfilter;
        wfunc->runCondition = NIL;
        wfunc->winstar = false;
        wfunc->winagg = true;
        wfunc->location = agg_ctor->location;

        // ORDER BY not supported in window functions
        if (agg_ctor->agg_order != NIL) {
            ereport(ERROR, "aggregate ORDER BY not implemented for window functions");
        }

        // Process window-specific transformation
        transformWindowFuncCall(pstate, wfunc, agg_ctor->over);
        node = (Node *) wfunc;
    } else {
        // Create regular aggregate
        Aggref *aggref = makeNode(Aggref);

        aggref->aggfnoid = aggfnoid;
        aggref->aggtype = aggtype;
        aggref->aggfilter = aggfilter;
        aggref->aggstar = false;
        aggref->aggvariadic = false;
        aggref->aggkind = AGGKIND_NORMAL;
        aggref->aggpresorted = false;
        aggref->aggsplit = AGGSPLIT_SIMPLE;
        aggref->aggno = -1;      // Set by planner
        aggref->aggtransno = -1; // Set by planner
        aggref->location = agg_ctor->location;

        // Process aggregate-specific transformation
        transformAggregateCall(pstate, aggref, args, agg_ctor->agg_order, false);
        node = (Node *) aggref;
    }

    // Wrap in JSON constructor expression
    return makeJsonConstructorExpr(pstate, ctor_type, NIL, (Expr *) node,
                                  returning, unique, absent_on_null,
                                  agg_ctor->location);
}
```