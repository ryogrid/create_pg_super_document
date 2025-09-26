# CaseExpr

## Location
[src/include/nodes/primnodes.h:1306-1317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1306-L1317)

## Overview
CaseExpr represents a SQL CASE expression, supporting both simple and searched CASE forms with conditional evaluation of multiple branches and an optional default result.

## Definition

```c
typedef struct CaseExpr
{
	Expr		xpr;
	/* type of expression result */
	Oid			casetype pg_node_attr(query_jumble_ignore);
	/* OID of collation, or InvalidOid if none */
	Oid			casecollid pg_node_attr(query_jumble_ignore);
	Expr	   *arg;			/* implicit equality comparison argument */
	List	   *args;			/* the arguments (list of WHEN clauses) */
	Expr	   *defresult;		/* the default result (ELSE clause) */
	ParseLoc	location;		/* token location, or -1 if unknown */
} CaseExpr;
```
## Detailed Description
CaseExpr implements SQL CASE expressions, which provide conditional logic within queries. PostgreSQL supports two distinct forms:

1. **Searched CASE**: 
   - The  field is NULL
   - Each WHEN clause contains a complete boolean expression
   
2. **Simple CASE**: 
   - The  field contains the test expression
   - Parse analysis transforms WHEN clauses into  comparisons

During parse analysis, the simple CASE form is transformed so that condition expressions become boolean comparisons using CaseTestExpr nodes. This allows the test expression to be evaluated only once, with CaseTestExpr serving as a placeholder for the test value in each WHEN condition.

The execution model uses conditional jumps: each WHEN clause is evaluated in sequence, and if a condition is true, the corresponding result is computed and control jumps to the end. If no conditions match, the ELSE result is used (transformCaseExpr always ensures a default exists).

## Parameters / Member Variables
- : Base expression node structure
- : OID of the result type for the CASE expression (ignored for query jumbling)
- : OID of the result collation, or InvalidOid if none (ignored for query jumbling)
- : Test expression for simple CASE form, NULL for searched CASE form
- : List of CaseWhen nodes representing WHEN clauses
- : Default result expression (ELSE clause)
- : Parse location in the original query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - CaseWhen (for individual WHEN clauses)
  - CaseTestExpr (placeholder for test values in simple CASE)
  - makeNode (for creating CaseExpr instances)
  - transformCaseExpr (parse-time transformation)
- Called from (representative examples):
  - transformCaseExpr (during parse analysis)
  - ExecInitExprRec (during execution plan initialization)  
  - eval_const_expressions_mutator (during constant folding)
  - assign_collations_walker (during collation analysis)

## Notes and Other Information
- Parse analysis completion can be tested by checking if casetype is InvalidOid
- transformCaseExpr always ensures a default result exists (adds NULL if needed)
- The executor uses a jump-based evaluation model for efficiency
- CaseTestExpr usage requires careful handling during function inlining to avoid conflicts
- Collation handling is special: the test expression's collation is not relevant to the result
- The args list contains CaseWhen nodes, each with a condition and result expression
- Both casetype and casecollid are ignored during query jumbling for plan stability
- Simple CASE forms are internally converted to searched CASE forms using CaseTestExpr placeholders
- Execution never evaluates unnecessary WHEN clauses due to the jump-based implementation