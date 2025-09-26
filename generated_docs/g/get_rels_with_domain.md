# get_rels_with_domain

## Location
[src/backend/commands/typecmds.c:3321-3489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3321-L3489)

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
  - [check_stack_depth](../c/check_stack_depth.md) (prevent stack overflow in recursion)
  - [table_open](../t/table_open.md)/relation_open/relation_close (relation access)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext/systable_endscan (catalog scanning)
  - [get_typtype](get_typtype.md) (determine if dependent type is a domain)
  - [find_composite_type_dependencies](../f/find_composite_type_dependencies.md) (check for container type usage)
  - [list_concat](../l/list_concat.md) (combine results from recursive calls)
  - [format_type_be](../f/format_type_be.md) (format domain type name for error messages)
- Called from:
  - [validateDomainCheckConstraint](../v/validateDomainCheckConstraint.md) (when validating check constraints)
  - [validateDomainNotNullConstraint](../v/validateDomainNotNullConstraint.md) (when validating NOT NULL constraints)
  - [get_rels_with_domain](get_rels_with_domain.md) (recursive calls for sub-domains)

## Notes and Other Information
- Contains known concurrency issues due to inability to lock domains during operation
- Risk of deadlocks when holding multiple relation locks simultaneously
- Does not support checking domain values inside container types (composite, array, range)
- Uses weakest suitable lock (typically ShareLock) to minimize deadlock risk
- Results are deterministic due to sorting attributes by column number
- Part of the domain constraint validation infrastructure