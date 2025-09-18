# get_rels_with_domain

## Location
src/backend/commands/typecmds.c: 3321 - 3489

## Overview
Discovers and returns all relations and their specific attribute numbers that use a given domain type, including relations that use derived domain types based on the target domain.

## Definition
```c
static List *get_rels_with_domain(Oid domainOid, LOCKMODE lockmode)
```

## Detailed Description
This function performs a comprehensive search through the dependency catalog (pg_depend) to identify all relations containing columns of a specified domain type. It supports nested domains by recursively processing derived domain types and builds a list of RelToCheck structures containing the relation and all relevant attribute numbers.

The function handles several important aspects:
- Recursively processes sub-domains that are based on the target domain
- Detects and reports errors for container types (composite types, arrays, ranges) that contain the domain
- Filters relations to include only tables and materialized views with user-defined columns
- Acquires the specified lock on each relation to prevent concurrent schema changes
- Returns attributes sorted by column number for predictable output

Key limitations include potential race conditions during concurrent DDL operations and the inability to check domain values within container types.

## Parameters / Member Variables
- `domainOid`: Object identifier of the domain type to search for
- `lockmode`: Type of lock to acquire on relations (must not be NoLock)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (prevent stack overflow in recursion)
  - table_open/relation_open/relation_close (relation access)
  - systable_beginscan/systable_getnext/systable_endscan (catalog scanning)
  - get_typtype (determine if dependent type is a domain)
  - find_composite_type_dependencies (check for container type usage)
  - list_concat (combine results from recursive calls)
  - format_type_be (format domain type name for error messages)
- Called from:
  - validateDomainCheckConstraint (when validating check constraints)
  - validateDomainNotNullConstraint (when validating NOT NULL constraints)
  - get_rels_with_domain (recursive calls for sub-domains)

## Notes and Other Information
- Contains known concurrency issues due to inability to lock domains during operation
- Risk of deadlocks when holding multiple relation locks simultaneously
- Does not support checking domain values inside container types (composite, array, range)
- Uses weakest suitable lock (typically ShareLock) to minimize deadlock risk
- Results are deterministic due to sorting attributes by column number
- Part of the domain constraint validation infrastructure