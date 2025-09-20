# TupleDescData

## Location
[src/include/access/tupdesc.h:79-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tupdesc.h#L79-L88)

## Overview
TupleDescData is the core structure that describes the layout and properties of tuples in PostgreSQL, containing attribute information, type metadata, and constraint details.

## Definition

```c
typedef struct TupleDescData
{
	int			natts;			/* number of attributes in the tuple */
	Oid			tdtypeid;		/* composite type ID for tuple type */
	int32		tdtypmod;		/* typmod for tuple type */
	int			tdrefcount;		/* reference count, or -1 if not counting */
	TupleConstr *constr;		/* constraints, or NULL if none */
	/* attrs[N] is the description of Attribute Number N+1 */
	FormData_pg_attribute attrs[FLEXIBLE_ARRAY_MEMBER];
}			TupleDescData;
```
## Detailed Description
TupleDescData is the fundamental structure used throughout PostgreSQL to describe the structure of tuples. It serves as a schema descriptor that contains all necessary information about a tuple's attributes, type information, constraints, and reference management. This structure is used for both persistent relations (tables) stored on disk and transient row types (such as query results).

The structure supports reference counting for cache management, allowing tuple descriptors to be safely shared and automatically cleaned up when no longer needed. For executor-created descriptors that don't need reference counting, tdrefcount is set to -1.

The structure is designed to efficiently handle cases where constraints can be omitted, making it suitable for temporary result sets that don't require full constraint validation.

## Parameters / Member Variables
- : Number of user attributes in the tuple (excludes system attributes)
- : OID identifying the composite type (RECORDOID for anonymous types)
- : Type modifier for the tuple type (-1 for named rowtypes, >= 0 for typcache lookup)
- : Reference count for cache management (-1 for non-counted descriptors)
- : Pointer to TupleConstr containing constraint information (NULL if none)
- : Flexible array of FormData_pg_attribute structures describing each attribute

## Dependencies
- Functions called/Symbols referenced:
  - [TupleConstr](TupleConstr.md)
  - FLEXIBLE_ARRAY_MEMBER
  - FormData_pg_attribute
- Called from (representative examples):
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [IndexScanDescData](../I/IndexScanDescData.md)
  - [TupleDesc](TupleDesc.md) (typedef)
  - TupleDescSize

## Notes and Other Information
- Central to PostgreSQL's type system and used extensively throughout the codebase
- Supports both persistent and transient tuple types with different constraint requirements
- Reference counting enables safe sharing in cache systems (relcache, typcache)
- The attrs array uses flexible array member syntax for variable-length allocation
- tdtypeid is never a domain type OID, even for domain-over-composite values
- System attributes are not included in the tuple descriptor
- Used for schema validation, tuple construction, and type checking operations