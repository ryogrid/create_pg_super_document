# SubLink

## Location
src/include/nodes/primnodes.h: 1008 - 1019

## Overview
SubLink represents a subselect appearing in an expression, along with its combining operators, and is later replaced by SubPlan nodes during query planning.

## Definition
```c
typedef enum SubLinkType
{
    EXISTS_SUBLINK,        /* EXISTS(SELECT ...) */
    ALL_SUBLINK,           /* (lefthand) op ALL (SELECT ...) */
    ANY_SUBLINK,           /* (lefthand) op ANY (SELECT ...) */
    ROWCOMPARE_SUBLINK,    /* (lefthand) op (SELECT ...) */
    EXPR_SUBLINK,          /* (SELECT with single targetlist item ...) */
    MULTIEXPR_SUBLINK,     /* (SELECT with multiple targetlist items ...) */
    ARRAY_SUBLINK,         /* ARRAY(SELECT with single targetlist item ...) */
    CTE_SUBLINK,           /* for SubPlans only */
} SubLinkType;

typedef struct SubLink
{
    Expr        xpr;
    SubLinkType subLinkType;    /* see above */
    int         subLinkId;      /* ID (1..n); 0 if not MULTIEXPR */
    Node       *testexpr;       /* outer-query test for ALL/ANY/ROWCOMPARE */
    /* originally specified operator name */
    List       *operName pg_node_attr(query_jumble_ignore);
    /* subselect as Query* or raw parsetree */
    Node       *subselect;
    ParseLoc    location;       /* token location, or -1 if unknown */
} SubLink;
```

## Detailed Description
SubLink is a crucial expression node that represents various forms of subqueries within SQL expressions. It handles eight different sublink types, each with specific semantics and requirements. The node serves as an intermediate representation during parsing and analysis, ultimately being replaced by executable SubPlan nodes during query planning.

The structure accommodates different subquery patterns: EXISTS sublinks test for row existence, ALL/ANY sublinks compare outer values against all/any inner values, ROWCOMPARE handles row-wise comparisons, EXPR/MULTIEXPR represent scalar subqueries, ARRAY sublinks create arrays from subquery results, and CTE_SUBLINK handles WITH queries (though only in SubPlans, not actual SubLink nodes).

During parse analysis, the parser transforms raw expressions: testexpr becomes a complete boolean expression with PARAM_SUBLINK nodes representing subquery output columns, and subselect transforms from raw parsetree to Query structure. This processed form is what appears in stored rules and throughout the rewriter.

## Parameters / Member Variables
- `xpr`: Base Expr node structure containing common expression fields
- `subLinkType`: Enumerated type indicating the form of subquery (EXISTS, ALL, ANY, ROWCOMPARE, EXPR, MULTIEXPR, ARRAY, CTE)
- `subLinkId`: Unique identifier for MULTIEXPR SubLinks within a targetlist, zero for other types
- `testexpr`: Outer-query test expression for ALL/ANY/ROWCOMPARE types, null for others
- `operName`: Originally specified operator name list, ignored during query jumbling
- `subselect`: The subquery, either as raw parsetree (during parsing) or Query structure (after analysis)
- `location`: Parse location of the sublink token in the original query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - SubLinkType
  - ParseLoc
- Called from (representative examples):
  - transformSubLink (during parse analysis)
  - transformMultiAssignRef (for multi-assignment updates)
  - process_sublinks_mutator (sublink processing)
  - convert_ANY_sublink_to_join (join conversion optimization)
  - convert_EXISTS_sublink_to_join (EXISTS to join conversion)
  - pull_up_sublinks_qual_recurse (sublink pullup optimization)
  - get_sublink_expr (query deparsing)
  - fireRIRonSubLink (rule processing)

## Notes and Other Information
- Not directly executable - must be replaced by SubPlan nodes during planning phase
- ALL/ANY/ROWCOMPARE require boolean-returning combining operators with specific semantics (AND for ALL, OR for ANY)
- ROWCOMPARE always has multiple lefthand expressions; single expressions become EXPR_SUBLINK instead
- EXPR/MULTIEXPR/ROWCOMPARE sublinks must return at most one row (NULL if no rows)
- ARRAY sublinks can return any number of rows to build result arrays
- MULTIEXPR sublinks enable multiple-assignment operations in UPDATE statements
- CTE_SUBLINK only appears in SubPlans for WITH query processing, never in actual SubLink nodes
- The subLinkId field enables correlation between MULTIEXPR SubLinks and PARAM_MULTIEXPR parameters in targetlists
- Location tracking supports accurate error reporting for subquery syntax and semantic issues