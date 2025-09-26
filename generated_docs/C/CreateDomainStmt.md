# CreateDomainStmt

## Location
[src/include/nodes/parsenodes.h:3156-3163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3156-L3163)

## Overview
CreateDomainStmt represents a CREATE DOMAIN statement in PostgreSQL parse tree, used to define a new domain type based on an existing data type with optional constraints.

## Definition
```c
typedef struct CreateDomainStmt
{
    NodeTag         type;
    List           *domainname;      /* qualified name (list of String) */
    TypeName       *typeName;        /* the base type */
    CollateClause  *collClause;      /* untransformed COLLATE spec, if any */
    List           *constraints;     /* constraints (list of Constraint nodes) */
} CreateDomainStmt;
```

## Detailed Description
CreateDomainStmt is a parse tree node structure that encapsulates information needed to create a domain type in PostgreSQL. Domains are user-defined data types based on existing types that can include additional constraints, default values, and collation specifications. This structure stores the domain name, base type, optional collation, and any constraints that should be applied to values of this domain type.

## Parameters / Member Variables
- `type`: NodeTag identifier for this parse node type
- `domainname`: List of String nodes representing the qualified name of the domain being created
- `typeName`: TypeName pointer specifying the underlying base data type for the domain
- `collClause`: CollateClause pointer for collation specification, or NULL if no collation specified
- `constraints`: List of Constraint nodes defining CHECK constraints, NOT NULL constraints, or DEFAULT values for the domain

## Dependencies
- Functions called/Symbols referenced:
  - TypeName (for base type specification)
  - CollateClause (for collation specification)
- Called from (representative examples):
  - DefineDomain
  - ProcessUtilitySlow
  - DEFAULT_TYPDELIM

## Notes and Other Information
- Part of PostgreSQL parse tree node hierarchy, inheriting from Node via NodeTag
- Used to implement CREATE DOMAIN functionality for user-defined constrained types
- Domains provide a way to create reusable type definitions with built-in validation rules
- The constraints list can contain CHECK expressions, NOT NULL specifications, and DEFAULT value definitions
- Collation can be specified to override the base type collation for text-based domains
- Located in src/include/nodes/parsenodes.h:3152-3163