# domain_check_safe

## Location
[src/backend/utils/adt/domains.c:355-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/domains.c#L355-L370)

## Overview
An error-safe variant of domain checking that validates whether a given value complies with domain constraints without throwing errors, instead reporting errors through an error context.

## Definition
```c
bool domain_check_safe(Datum value, bool isnull, Oid domainType,
                      void **extra, MemoryContext mcxt,
                      Node *escontext)
```

## Detailed Description
`domain_check_safe` is a wrapper function that provides error-safe domain constraint validation. It serves as the non-throwing variant of domain checking functionality, allowing callers to handle constraint violations gracefully through the error context mechanism instead of having errors thrown as exceptions. This function is particularly useful in contexts where domain constraint failures should not interrupt processing but should be reported through structured error handling.

The function directly delegates to `domain_check_internal` with the provided error context, making it a thin wrapper that enables safe domain validation in error-sensitive operations.

## Parameters / Member Variables
- `value`: The Datum value to be validated against domain constraints
- `isnull`: Boolean indicating whether the value is NULL
- `domainType`: OID of the domain type whose constraints should be checked
- `extra`: Pointer to extra cache information for constraint checking (can be NULL)
- `mcxt`: Memory context for any temporary allocations during checking
- `escontext`: Error context node for soft error reporting instead of throwing exceptions

## Dependencies
- Functions called/Symbols referenced:
  - [domain_check_internal](domain_check_internal.md)
- Called from (representative examples):
  - [ExecEvalJsonCoercion](../E/ExecEvalJsonCoercion.md)
  - [populate_composite](../p/populate_composite.md)
  - [populate_domain](../p/populate_domain.md)
  - [populate_recordset_record](../p/populate_recordset_record.md)

## Notes and Other Information
- This is a thin wrapper around `domain_check_internal` that enables error-safe domain validation
- The function is primarily used in JSON processing functions where constraint violations should not halt processing
- Returns `true` if the value passes domain constraints, `false` if it fails (with error details stored in escontext)
- Located at src/backend/utils/adt/domains.c:355-370