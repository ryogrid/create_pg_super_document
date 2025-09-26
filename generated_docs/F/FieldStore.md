# FieldStore

## Location
src/include/nodes/primnodes.h: 1156 - 1166

## Overview
FieldStore represents the operation of modifying one or more fields in a tuple value, yielding a new tuple value without modifying the input, primarily used for implementing UPDATE operations on composite types.

## Definition

```c
typedef struct FieldStore
{
	Expr		xpr;
	Expr	   *arg;			/* input tuple value */
	List	   *newvals;		/* new value(s) for field(s) */
	/* integer list of field attnums */
	List	   *fieldnums pg_node_attr(query_jumble_ignore);
	/* type of result (same as type of arg) */
	Oid			resulttype pg_node_attr(query_jumble_ignore);
	/* Like RowExpr, we deliberately omit a typmod and collation here */
} FieldStore;
```
## Detailed Description
FieldStore implements field modification operations for composite types, creating a new tuple value with updated fields while leaving the original input unchanged. This is functionally similar to the assign case of SubscriptingRef and is primarily used to implement UPDATE operations on portions of composite columns.

The operation takes an input tuple and produces a new tuple with specified fields replaced by new values. The resulttype is always a named composite type (not a domain) - to update a composite domain value, a CoerceToDomain node must be applied to the FieldStore result.

While the parser generates FieldStores with single-element lists for individual field updates, the planner optimizes multiple updates to the same base column by collapsing them into a single FieldStore node with multiple fields and values.

## Parameters / Member Variables
- : Base Expr node structure
- : Input tuple value expression to be modified
- : List of new value expressions for the fields being updated
- : List of integers representing attribute numbers of fields to be modified
- : OID of the result type (same as the type of the input arg)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - ExecInitExprRec
  - transformAssignmentIndirection
  - process_matched_tle
  - get_assignment_input
  - isAssignmentIndirectionExpr

## Notes and Other Information
- The operation is non-destructive - the input tuple is not modified, a new tuple is created
- Multiple fields can be updated in a single FieldStore operation
- The parser generates single-field FieldStores, but the planner can collapse multiple field updates
- Used for implementing UPDATE statements on composite columns or record fields
- resulttype is always a named composite type, not a domain type
- Like RowExpr, typmod and collation are deliberately omitted from the result type information
- Essential for supporting partial updates of complex data types in PostgreSQL
- The fieldnums and resulttype have query_jumble_ignore attributes for plan caching optimization