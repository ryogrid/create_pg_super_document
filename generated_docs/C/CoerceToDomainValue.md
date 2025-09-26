# CoerceToDomainValue

## Location
[src/include/nodes/primnodes.h:2048-2059](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L2048-L2059)

## Overview
CoerceToDomainValue is a placeholder node representing the value to be processed by a domain's check constraint during domain type validation in PostgreSQL.

## Definition

```c
typedef struct CoerceToDomainValue
{
	Expr		xpr;
	/* type for substituted value */
	Oid			typeId;
	/* typemod for substituted value */
	int32		typeMod pg_node_attr(query_jumble_ignore);
	/* collation for the substituted value */
	Oid			collation pg_node_attr(query_jumble_ignore);
	/* token location, or -1 if unknown */
	ParseLoc	location;
} CoerceToDomainValue;
```
## Detailed Description
CoerceToDomainValue is a specialized placeholder node that acts as a substitute for the actual value being tested during domain constraint validation. It functions similarly to a Param node but is implemented more simply since only one replacement value is needed at a time during constraint checking.

This node is used within domain check constraint expressions where it represents the VALUE keyword in constraint definitions. When a domain constraint is evaluated, this placeholder is replaced with the actual value being coerced to the domain type.

A critical design aspect is that the typeId/typeMod/collation fields are set from the domain's base type, not the domain itself. This is intentional because the value should not be considered a member of the domain until all constraints have been successfully validated.

## Parameters / Member Variables
- `xpr`: Base Expr node structure
- `typeId`: OID of the type for the substituted value (from domain's base type)
- `pg_node_attr(query_jumble_ignore)`: Type modifier for the substituted value
- `pg_node_attr(query_jumble_ignore)`: OID of the collation for the substituted value
- `location`: Token location in source query, or -1 if unknown
## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc (for location tracking)
  - [Expr](../E/Expr.md) (base expression structure)
  - Oid (object identifier type)
  
- Called from (representative examples):
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md) (when adding check constraints to domains)
  - [replace_domain_constraint_value](../r/replace_domain_constraint_value.md) (constraint value replacement)
  - [exprType](../e/exprType.md) (expression type determination)
  - [exprTypmod](../e/exprTypmod.md) (expression type modifier determination)
  - [exprCollation](../e/exprCollation.md) (expression collation determination)

## Notes and Other Information
- Essential component of PostgreSQL's domain constraint system
- Acts as the VALUE placeholder in domain CHECK constraint expressions
- The pg_node_attr(query_jumble_ignore) annotations on typeMod and collation indicate these fields should be ignored during query fingerprinting
- Used primarily during domain constraint validation and constraint manipulation operations
- The distinction between base type properties and domain type properties is crucial for correct constraint semantics
- Simpler implementation compared to Param nodes due to the single-value replacement pattern
- Critical for implementing SQL standard domain constraints with proper VALUE semantics