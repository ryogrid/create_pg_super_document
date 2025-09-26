# FieldSelect

## Location
[src/include/nodes/primnodes.h:1125-1136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1125-L1136)

## Overview
FieldSelect represents the operation of extracting one field from a tuple value, taking a rowtype Datum as input and returning the specified field as a Datum.

## Definition

```c
typedef struct FieldSelect
{
	Expr		xpr;
	Expr	   *arg;			/* input expression */
	AttrNumber	fieldnum;		/* attribute number of field to extract */
	/* type of the field (result type of this node) */
	Oid			resulttype pg_node_attr(query_jumble_ignore);
	/* output typmod (usually -1) */
	int32		resulttypmod pg_node_attr(query_jumble_ignore);
	/* OID of collation of the field */
	Oid			resultcollid pg_node_attr(query_jumble_ignore);
} FieldSelect;
```
## Detailed Description
FieldSelect is an expression node that implements field extraction from composite/row types. At runtime, the input expression (arg) is expected to yield a rowtype Datum. The node extracts the field specified by fieldnum and returns it as a Datum of the appropriate type.

This operation is fundamental for accessing individual columns from composite types, row expressions, or record values. The node contains complete type information for the extracted field, including the result type, typmod, and collation.

The query_jumble_ignore attributes on the type-related fields indicate that these fields should be ignored when generating query fingerprints for plan caching, as they represent derived type information rather than the core operation structure.

## Parameters / Member Variables
- : Base Expr node structure
- : Input expression that should yield a rowtype Datum
- : Attribute number of the field to extract from the composite value
- : OID of the type of the extracted field (result type of this node)
- : Type modifier for the result type (usually -1 if not applicable)
- : OID of the collation for the extracted field

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - ExecInitExprRec
  - eval_const_expressions_mutator
  - ParseComplexProjection
  - ExpandRowReference
  - get_rule_expr

## Notes and Other Information
- Used for accessing individual fields from composite types, records, and row expressions
- The fieldnum is an attribute number identifying which field to extract
- Type information (resulttype, resulttypmod, resultcollid) is stored for type checking and execution
- query_jumble_ignore attributes prevent type information from affecting query plan caching fingerprints
- Commonly generated when parsing dot-notation field access (e.g., record.field)
- Essential for decomposing composite values in SQL expressions