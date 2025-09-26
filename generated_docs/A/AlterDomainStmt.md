# AlterDomainStmt

## Location
[src/include/nodes/parsenodes.h:2461-2477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2461-L2477)

## Overview
AlterDomainStmt represents the parsed form of an ALTER DOMAIN statement, used to modify domain type definitions including constraints, defaults, and null behavior.

## Definition

```c
typedef struct AlterDomainStmt
{
	NodeTag		type;
	char		subtype;		/*------------
								 *	T = alter column default
								 *	N = alter column drop not null
								 *	O = alter column set not null
								 *	C = add constraint
								 *	X = drop constraint
								 *------------
								 */
	List	   *typeName;		/* domain to work on */
	char	   *name;			/* column or constraint name to act on */
	Node	   *def;			/* definition of default or constraint */
	DropBehavior behavior;		/* RESTRICT or CASCADE for DROP cases */
	bool		missing_ok;		/* skip error if missing? */
} AlterDomainStmt;
```
## Detailed Description
AlterDomainStmt is a parse tree node structure that represents ALTER DOMAIN SQL commands. Domains in PostgreSQL are user-defined data types that are based on existing types but can include additional constraints and default values. This structure provides a unified representation for various domain modification operations.

The structure uses a character-based subtype field to distinguish between different types of alterations, making it a polymorphic structure that can represent multiple distinct operations within a single node type. This design pattern is common in PostgreSQL's parser for statements that have multiple variants.

The structure supports the full range of domain alterations including setting or dropping default values, modifying null constraints, and adding or removing check constraints. The design allows for both destructive operations (with CASCADE semantics) and safe operations that respect dependencies.

## Parameters / Member Variables
- `type`: NodeTag for node type identification in PostgreSQL's node system
- `subtype`: Character code indicating the specific type of alteration:
  - 'T': Alter column default value
  - 'N': Drop NOT NULL constraint
  - 'O': Set NOT NULL constraint
  - 'C': Add constraint
  - 'X': Drop constraint
- `typeName`: List of strings representing the qualified name of the domain to be altered
- `name`: String identifier for the column or constraint name being acted upon
- `def`: Generic Node pointer containing the definition of new defaults or constraints
- `behavior`: DropBehavior enum (RESTRICT or CASCADE) controlling how dependent objects are handled during DROP operations
- `missing_ok`: Boolean flag indicating whether to skip errors if the target object doesn't exist (IF EXISTS semantics)

## Dependencies
- Functions called/Symbols referenced:
  - DropBehavior
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - [ATPostAlterTypeParse](ATPostAlterTypeParse.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Part of PostgreSQL's parse tree node system, inheriting from the standard Node structure
- The subtype field uses single characters for efficiency and historical reasons
- Domains are a PostgreSQL extension to standard SQL, providing a way to create reusable custom types
- The structure design mirrors similar patterns used in other ALTER statement nodes
- Domain alterations can cascade to columns that use the domain type, controlled by the behavior field
- Used in conjunction with PostgreSQL's type system and constraint checking infrastructure