# _typeInfo

## Location
[src/bin/pg_dump/pg_dump.h:197-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L197-L222)

## Overview
The _typeInfo structure represents type information used by the PostgreSQL pg_dump utility to store metadata about database types during the dump process.

## Definition


## Detailed Description
The _typeInfo structure is a comprehensive data structure used by pg_dump to manage type information during database dumping operations. It extends the base DumpableObject and DumpableAcl structures to provide type-specific metadata. This structure handles various PostgreSQL type categories including base types, composite types, domains, arrays, and multirange types. It maintains both raw type names and formatted type names, supports shell type references for forward declarations, and includes specialized fields for domain constraints.

## Parameters / Member Variables
- : Base dumpable object structure containing common dump metadata
- : Access control list information for the type
- : Formatted type name (quoted and potentially schema-qualified)
- : Role/owner name of the type
- : OID of the element type (for arrays and ranges)
- : OID of the relation associated with this type (for composite types)
- : Relation kind character ('r' for table, 'v' for view, 'c' for composite, etc.)
- : Type category character ('b' for base, 'c' for composite, 'd' for domain, etc.)
- : Boolean flag indicating if this is an auto-generated array type
- : Boolean flag indicating if this is an auto-generated multirange type
- : Boolean flag indicating if the type is fully defined (typisdefined)
- : Pointer to associated shell type entry, used for forward declarations
- : Pointer to not-null constraint information (for domain types)
- : Number of CHECK constraints for domain types
- : Array of pointers to CHECK constraint information for domains

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
  - [_shellTypeInfo](../s/_shellTypeInfo.md)
  - [_constraintInfo](../c/_constraintInfo.md)
- Called from (representative examples):
  - No direct references found (likely used internally by pg_dump functions)

## Notes and Other Information
- This structure is central to pg_dump's type management system
- The distinction between dobj.name (raw typname) and ftypname (formatted) allows proper handling of schema-qualified and quoted type names
- Shell types are used to handle forward references in type dependencies
- Domain-specific fields (notnull, nDomChecks, domChecks) enable proper constraint dumping
- The structure supports PostgreSQL's rich type system including composite types, domains, arrays, and the newer multirange types