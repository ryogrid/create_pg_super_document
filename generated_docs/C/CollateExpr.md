# CollateExpr

## Location
src/include/nodes/primnodes.h: 1276 - 1282

## Overview
CollateExpr represents a COLLATE clause in SQL expressions, specifying the collation to be used for string comparison and sorting operations on the wrapped expression.

## Definition

```c
typedef struct CollateExpr
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	Oid			collOid;		/* collation's OID */
	ParseLoc	location;		/* token location, or -1 if unknown */
} CollateExpr;
```
## Detailed Description
CollateExpr is a parse-time representation of SQL COLLATE clauses (e.g., ). It wraps an expression to specify that a particular collation should be applied when the expression is used in contexts requiring string comparison, sorting, or other collation-dependent operations.

An important characteristic of CollateExpr is that it exists only during parsing and early planning phases. The planner replaces CollateExpr nodes with RelabelType nodes during expression preprocessing, so the executor never encounters CollateExpr nodes directly. This replacement improves expression uniformity and simplifies comparison operations.

The transformation process:
1. Parse analysis creates CollateExpr nodes for explicit COLLATE clauses
2. During parse-time collation assignment, CollateExpr may be stripped and reapplied as needed
3. The planner converts CollateExpr to RelabelType with appropriate collation information
4. The executor works with RelabelType nodes that carry collation metadata

## Parameters / Member Variables
- : Base expression node structure
- : The input expression to which the collation applies
- : OID of the collation to be applied to the expression
- : Parse location in the original query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating CollateExpr instances)
  - type_is_collatable (to validate collation applicability)
  - exprCollation (to determine expression collations)
  - RelabelType (as replacement target during planning)
- Called from (representative examples):
  - coerce_to_target_type (during type coercion with collation handling)
  - assign_collations_walker (during collation assignment)
  - eval_const_expressions_mutator (during constant folding and expression preprocessing)

## Notes and Other Information
- CollateExpr exists only during parsing and early planning phases
- The planner systematically replaces CollateExpr with RelabelType for execution efficiency
- Multiple stacked CollateExprs are simplified to keep only the topmost one
- If the target type is not collatable, CollateExpr nodes are discarded entirely
- Used to implement SQL standard COLLATE clause functionality
- The replacement with RelabelType improves expression uniformity and comparison efficiency
- Parse-time collation changes should ideally use CollateExpr, but some cases use RelabelType for practical reasons
- Never appears in execution plans - all collation information is carried by RelabelType nodes in executed expressions