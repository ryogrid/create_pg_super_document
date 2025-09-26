# CreateOpClassItem

## Location
[src/include/nodes/parsenodes.h:3184-3195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3184-L3195)

## Overview
CreateOpClassItem represents a single item within an operator class definition, specifying operators, functions, or storage types that comprise the operator class.

## Definition

```c
typedef struct CreateOpClassItem
{
	NodeTag		type;
	int			itemtype;		/* see codes above */
	ObjectWithArgs *name;		/* operator or function name and args */
	int			number;			/* strategy num or support proc num */
	List	   *order_family;	/* only used for ordering operators */
	List	   *class_args;		/* amproclefttype/amprocrighttype or
								 * amoplefttype/amoprighttype */
	/* fields used for a storagetype item: */
	TypeName   *storedtype;		/* datatype stored in index */
} CreateOpClassItem;
```
## Detailed Description
CreateOpClassItem is a parse tree node that represents individual components of an operator class during its creation or modification. Operator classes define sets of operators and support functions that an access method can use for a particular data type. Each CreateOpClassItem specifies either an operator, a support function, or a storage type declaration within the operator class.

The structure supports three types of items:
- **OPCLASS_ITEM_OPERATOR (1)**: Defines an operator with its strategy number
- **OPCLASS_ITEM_FUNCTION (2)**: Defines a support function with its procedure number  
- **OPCLASS_ITEM_STORAGETYPE (3)**: Specifies the storage data type for the index

## Parameters / Member Variables
- `type`: NodeTag identifier for this parse tree node type
- `itemtype`: Item type code (OPCLASS_ITEM_OPERATOR, OPCLASS_ITEM_FUNCTION, or OPCLASS_ITEM_STORAGETYPE)
- `*name`: ObjectWithArgs structure containing the operator or function name and its arguments
- `number`: Strategy number for operators or support procedure number for functions
- `*order_family`: List used specifically for ordering operators to specify operator family relationships
- `*class_args`: List containing argument types (amproclefttype/amprocrighttype for procedures, amoplefttype/amoprighttype for operators)
- `*storedtype`: TypeName specifying the datatype stored in the index (used only for storage type items)
## Dependencies
- Functions called/Symbols referenced:
  - [ObjectWithArgs](../O/ObjectWithArgs.md)
  - [TypeName](../T/TypeName.md)
- Called from (representative examples):
  - [DefineOpClass](../D/DefineOpClass.md)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md)
  - [AlterOpFamilyDrop](../A/AlterOpFamilyDrop.md)

## Notes and Other Information
- This structure is primarily used during CREATE OPERATOR CLASS and ALTER OPERATOR FAMILY operations
- The itemtype field determines which other fields are meaningful for a particular item
- For operator items, the number field contains the strategy number
- For function items, the number field contains the support procedure number
- The order_family field is specifically used for operators that participate in ordering relationships
- Storage type items use only the storedtype field, with other fields being irrelevant