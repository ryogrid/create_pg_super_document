# domainAddCheckConstraint

## Location
src/backend/commands/typecmds.c: 3510 - 3636

## Overview
Creates and stores a check constraint for a domain type, handling constraint name generation, expression transformation, validation, and catalog entry creation.

## Definition
```c
static char *domainAddCheckConstraint(Oid domainOid, Oid domainNamespace, Oid baseTypeOid,
                                    int typMod, Constraint *constr,
                                    const char *domainName, ObjectAddress *constrAddr)
```

## Detailed Description
This function serves as the core implementation for adding check constraints to domain types, used by both CREATE DOMAIN and ALTER DOMAIN ADD CONSTRAINT commands. It performs comprehensive constraint processing including name validation and generation, expression parsing and transformation, semantic validation, and catalog storage.

The function transforms raw SQL expressions into executable constraint expressions by:
- Creating a parse state for expression processing
- Setting up a CoerceToDomainValue node to represent VALUE references in the constraint
- Transforming the raw expression through the parser
- Validating that the expression yields a boolean result
- Ensuring no table references are used in the constraint
- Converting the validated expression to string form for storage

The function integrates with PostgreSQL's constraint management system by creating appropriate catalog entries and establishing object relationships.

## Parameters / Member Variables
- `domainOid`: Object identifier of the domain receiving the constraint
- `domainNamespace`: Namespace (schema) containing the domain
- `baseTypeOid`: Object identifier of the domain's underlying base type
- `typMod`: Type modifier for the base type (-1 if none)
- `constr`: Constraint structure containing the raw constraint definition
- `domainName`: Name of the domain (used for error messages and name generation)
- `constrAddr`: Optional output parameter for the constraint's object address

## Dependencies
- Functions called/Symbols referenced:
  - ConstraintNameIsUsed/ChooseConstraintName (constraint name management)
  - make_parsestate (create parser state)
  - makeNode (create CoerceToDomainValue node)
  - replace_domain_constraint_value (hook for VALUE substitution)
  - transformExpr (parse and transform expressions)
  - coerce_to_boolean (ensure boolean result type)
  - assign_expr_collations (handle collation information)
  - contain_var_clause (check for forbidden table references)
  - nodeToString (serialize expression for storage)
  - CreateConstraintEntry (create catalog entry)
  - ObjectAddressSet (set output object address)
- Called from:
  - DefineDomain (during CREATE DOMAIN)
  - AlterDomainAddConstraint (during ALTER DOMAIN ADD CONSTRAINT)

## Notes and Other Information
- Returns the binary (nodeToString) representation of the constraint expression
- Supports both named and automatically-generated constraint names
- Validates that constraints contain no table references (domain-level only)
- Uses CoerceToDomainValue to represent VALUE in constraint expressions
- Creates non-deferrable, immediately validated constraints by default
- Integrates with PostgreSQL's dependency tracking system through CreateConstraintEntry
- Part of the domain constraint management infrastructure