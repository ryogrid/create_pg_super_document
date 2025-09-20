# PLyObToDomain

## Location
[src/pl/plpython/plpy_typeio.h:119-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.h#L119-L123)

## Overview
A structure used in PostgreSQL's PL/Python extension to store conversion information for domain types when converting Python objects to PostgreSQL Datum values.

## Definition

```c
typedef struct PLyObToDomain
{
	PLyObToDatum *base;			/* conversion info for domain's base type */
	void	   *domain_info;	/* cache space for domain_check() */
} PLyObToDomain;
```
## Detailed Description
PLyObToDomain is a specialized structure within the PL/Python type conversion system that handles the conversion of Python objects to PostgreSQL domain types. Domain types in PostgreSQL are user-defined types based on existing base types with additional constraints. This structure provides the necessary information to perform both the base type conversion and domain constraint validation.

The structure serves as a bridge between Python objects and PostgreSQL's domain type system, ensuring that converted values not only match the underlying base type but also satisfy any domain-specific constraints that may have been defined.

## Parameters / Member Variables
- : Pointer to PLyObToDatum structure containing conversion information for the domain's underlying base type
- : Generic pointer to cached information used by domain_check() function for constraint validation

## Dependencies
- Functions called/Symbols referenced:
  - [PLyObToDatum](PLyObToDatum.md) (base type conversion structure)
- Called from (representative examples):
  - [PLyObToDatum](PLyObToDatum.md) (as part of union in conversion structure)

## Notes and Other Information
- This structure is part of the PL/Python type conversion framework located in src/pl/plpython/plpy_typeio.h
- Used specifically for handling PostgreSQL domain types, which are constrained versions of base types
- The domain_info member provides caching for domain constraint checking to improve performance
- Integrated into the larger PLyObToDatum conversion system through a union structure