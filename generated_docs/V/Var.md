# Var

## Location
[src/include/nodes/primnodes.h:247-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L247-L294)

## Overview
The Var structure represents a variable in PostgreSQL's expression tree, typically referring to a column in a table or a derived value from a subquery.

## Definition

```c
typedef struct Var
{
	Expr		xpr;

	/*
	 * index of this var's relation in the range table, or
	 * INNER_VAR/OUTER_VAR/etc
	 */
	int			varno;

	/*
	 * attribute number of this var, or zero for all attrs ("whole-row Var")
	 */
	AttrNumber	varattno;

	/* pg_type OID for the type of this var */
	Oid			vartype pg_node_attr(query_jumble_ignore);
	/* pg_attribute typmod value */
	int32		vartypmod pg_node_attr(query_jumble_ignore);
	/* OID of collation, or InvalidOid if none */
	Oid			varcollid pg_node_attr(query_jumble_ignore);

	/*
	 * RT indexes of outer joins that can replace the Var's value with null.
	 * We can omit varnullingrels in the query jumble, because it's fully
	 * determined by varno/varlevelsup plus the Var's query location.
	 */
	Bitmapset  *varnullingrels pg_node_attr(query_jumble_ignore);

	/*
	 * for subquery variables referencing outer relations; 0 in a normal var,
	 * >0 means N levels up
	 */
	Index		varlevelsup;

	/*
	 * varnosyn/varattnosyn are ignored for equality, because Vars with
	 * different syntactic identifiers are semantically the same as long as
	 * their varno/varattno match.
	 */
	/* syntactic relation index (0 if unknown) */
	Index		varnosyn pg_node_attr(equal_ignore, query_jumble_ignore);
	/* syntactic attribute number */
	AttrNumber	varattnosyn pg_node_attr(equal_ignore, query_jumble_ignore);

	/* token location, or -1 if unknown */
	ParseLoc	location;
} Var;
```
## Detailed Description
The Var structure is a fundamental expression node type in PostgreSQL's query processing system. It represents references to table columns, computed expressions, or values from outer query levels. Vars are created during query parsing and planning phases and are used throughout the execution pipeline to identify and access specific data values. The structure includes both semantic information (what the variable actually refers to) and syntactic information (how it appeared in the original query text).

## Parameters / Member Variables
- : Base expression node structure containing common expression properties
- : Index of this variable's relation in the range table, or special values like INNER_VAR/OUTER_VAR for join contexts
- : Attribute number of this variable, or zero for whole-row references (all attributes)
- : PostgreSQL type system OID for the data type of this variable
- : Type modifier value from pg_attribute, providing additional type information
- : OID of the collation for this variable, or InvalidOid if no collation applies
- : Bitmap set of outer join relation indexes that can potentially set this variable's value to NULL
- : Nesting level for subquery variables (0 for current level, >0 for outer query levels)
- : Syntactic relation index as it appeared in the original query (0 if unknown)
- : Syntactic attribute number as it appeared in the original query
- : Token location in the original query text, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - Used throughout the PostgreSQL query planning and execution system
  - Referenced in expression evaluation, optimization, and execution contexts

## Notes and Other Information
- The structure includes pg_node_attr annotations that control behavior during query jumbling and equality comparisons
- Syntactic fields (varnosyn/varattnosyn) are ignored during equality checks since semantic equivalence is determined by varno/varattno
- The varnullingrels field supports PostgreSQL's handling of outer joins and NULL value propagation
- This is a core data structure in PostgreSQL's expression system and appears extensively throughout the codebase