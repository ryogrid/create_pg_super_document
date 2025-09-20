# RelToCheck

## Location
[src/backend/commands/typecmds.c:85-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L85-L105)

## Overview
RelToCheck is a result structure used to represent relations (tables) that contain columns of a specific domain type, along with information about which attributes need to be checked during domain validation operations.

## Definition

```c
structure for AlterTypeRecurse() */
typedef struct
{
	/* Flags indicating which type attributes to update */
	bool		updateStorage;
	bool		updateReceive;
	bool		updateSend;
	bool		updateTypmodin;
	bool		updateTypmodout;
	bool		updateAnalyze;
	bool		updateSubscript;
	/* New values for relevant attributes */
	char		storage;
	Oid			receiveOid;
	Oid			sendOid;
	Oid			typmodinOid;
	Oid			typmodoutOid;
	Oid			analyzeOid;
	Oid			subscriptOid;
} AlterTypeRecurseParams;
```
## Detailed Description
RelToCheck is a supporting data structure used in PostgreSQL's type system management, specifically for domain type operations. It is primarily used by the  function to collect and organize information about relations that contain columns of a particular domain type. This structure facilitates efficient processing of domain-related operations such as validation, constraint checking, and type modifications by maintaining a list of relations and their relevant attributes that need to be examined.

The structure serves as an optimization mechanism that groups multiple attributes of the same relation together, avoiding redundant relation opening and locking operations. It is particularly important during domain validation processes where PostgreSQL needs to verify that all existing data in tables using a domain type still conforms to modified domain constraints.

## Parameters / Member Variables
- : An opened and locked Relation object representing the table that contains columns of the domain type being processed
- : The count of attributes (columns) in this relation that are of the target domain type or derived from it
- : A dynamically allocated array containing the attribute numbers (column numbers) that are of interest for the current domain operation

## Dependencies
- Functions called/Symbols referenced:
  - [Relation](Relation.md) (PostgreSQL relation type)
  - RelationGetNumberOfAttributes
- Called from (representative examples):
  - [get_rels_with_domain](../g/get_rels_with_domain.md)
  - [validateDomainNotNullConstraint](../v/validateDomainNotNullConstraint.md)
  - [validateDomainCheckConstraint](../v/validateDomainCheckConstraint.md)

## Notes and Other Information
- The  array is allocated with enough space for all attributes in the relation (), but only the first  entries are actually used
- This structure is typically used as part of a list where each entry represents one relation that needs to be processed
- The relation is kept open and locked while the structure exists to ensure consistency during domain operations
- Memory management for this structure and its  array is handled by PostgreSQL's memory context system using 
- The structure is primarily used internally within  for domain-related DDL operations