# AlterOpFamilyStmt

## Location
src/include/nodes/parsenodes.h: 3212 - 3219

## Overview
AlterOpFamilyStmt represents an ALTER OPERATOR FAMILY statement in the PostgreSQL parser, allowing addition or removal of operators and functions from an existing operator family.

## Definition

```c
typedef struct AlterOpFamilyStmt
{
	NodeTag		type;
	List	   *opfamilyname;	/* qualified name (list of String) */
	char	   *amname;			/* name of index AM opfamily is for */
	bool		isDrop;			/* ADD or DROP the items? */
	List	   *items;			/* List of CreateOpClassItem nodes */
} AlterOpFamilyStmt;
```
## Detailed Description
AlterOpFamilyStmt is a parse tree node that represents the ALTER OPERATOR FAMILY SQL statement. This statement allows modification of existing operator families by adding or dropping operators, support functions, or storage types. The statement can perform either ADD operations (to include new items in the family) or DROP operations (to remove existing items from the family).

The structure reuses CreateOpClassItem nodes to represent the items being added or dropped, providing a consistent interface for specifying operators, functions, and storage types. The isDrop flag determines whether the operation is additive or subtractive, allowing the same parser structure to handle both variants of the ALTER OPERATOR FAMILY statement.

## Parameters / Member Variables
- : NodeTag identifier for this parse tree node type
- : List of String nodes representing the qualified name of the operator family (schema.name)
- : String containing the name of the index access method the operator family belongs to
- : Boolean flag indicating the operation type (true for DROP, false for ADD)
- : List of CreateOpClassItem nodes specifying the operators, functions, or storage types to add or drop

## Dependencies
- Functions called/Symbols referenced:
  - (Uses CreateOpClassItem nodes in items list)
- Called from (representative examples):
  - EventTriggerCollectAlterOpFam
  - AlterOpFamily
  - AlterOpFamilyAdd
  - AlterOpFamilyDrop
  - ProcessUtilitySlow

## Notes and Other Information
- The same structure handles both ADD and DROP variants of ALTER OPERATOR FAMILY
- Items being added or dropped are represented using CreateOpClassItem nodes for consistency
- The amname must match the access method of the existing operator family
- ADD operations can include operators, support functions, and storage type specifications
- DROP operations typically only need to specify the operator or function signature to identify what to remove
- This statement is commonly used to extend operator families with cross-data-type operators
- Event triggers can intercept these operations for auditing or additional processing