# CreateOpFamilyStmt

## Location
[src/include/nodes/parsenodes.h:3201-3206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3201-L3206)

## Overview
CreateOpFamilyStmt represents a CREATE OPERATOR FAMILY statement in the PostgreSQL parser, defining a new operator family for a specific access method.

## Definition

```c
typedef struct CreateOpFamilyStmt
{
	NodeTag		type;
	List	   *opfamilyname;	/* qualified name (list of String) */
	char	   *amname;			/* name of index AM opfamily is for */
} CreateOpFamilyStmt;
```
## Detailed Description
CreateOpFamilyStmt is a parse tree node that represents the CREATE OPERATOR FAMILY SQL statement. Operator families are collections of operator classes and operators that work together for a particular access method (like B-tree, Hash, GiST, etc.). The operator family provides a way to group related operators and operator classes, allowing for more flexible indexing strategies and cross-data-type operations.

When a CREATE OPERATOR FAMILY statement is parsed, it creates this structure which contains the essential information needed to create the operator family: its qualified name and the access method it belongs to. The actual creation of the operator family in the system catalogs is handled by the DefineOpFamily function.

## Parameters / Member Variables
- : NodeTag identifier for this parse tree node type
- : List of String nodes representing the qualified name of the operator family (schema.name)
- : String containing the name of the index access method (e.g., 'btree', 'hash', 'gist', 'gin', 'spgist', 'brin')

## Dependencies
- Functions called/Symbols referenced:
  - (None directly - uses basic List and string types)
- Called from (representative examples):
  - CreateOpFamily
  - DefineOpClass
  - DefineOpFamily
  - ProcessUtilitySlow

## Notes and Other Information
- Operator families are a higher-level abstraction than operator classes, allowing multiple operator classes to share operators
- The amname must correspond to an existing access method in the system
- Operator families enable cross-data-type operations, such as comparing integers and bigints in the same B-tree index
- This statement typically precedes CREATE OPERATOR CLASS statements that will belong to the family
- The qualified name handling allows operator families to be created in specific schemas