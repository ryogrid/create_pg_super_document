# domain_check_internal

## Location
src/backend/utils/adt/domains.c: 371 - 406

## Overview
The core workhorse function that performs domain constraint validation for both error-throwing and error-safe variants of domain checking.

## Definition
```c
static bool domain_check_internal(Datum value, bool isnull, Oid domainType,
                                 void **extra, MemoryContext mcxt,
                                 Node *escontext)
```

## Detailed Description
`domain_check_internal` is the internal implementation that handles the actual domain constraint validation logic for PostgreSQL domains. It serves as the common backend for both `domain_check()` and `domain_check_safe()` functions, providing a unified constraint checking mechanism.

The function employs caching optimization by maintaining domain state information across multiple calls via the `extra` parameter. When domain state setup is needed, it calls `domain_state_setup()` to initialize the necessary constraint checking data structures. The actual validation is performed by `domain_check_input()`, with error handling determined by whether an error context is provided.

The function supports both hard error (exception throwing) and soft error (error context) modes, making it flexible for different use cases within the PostgreSQL system.

## Parameters / Member Variables
- `value`: The Datum value to be validated against domain constraints
- `isnull`: Boolean flag indicating whether the value is NULL
- `domainType`: OID of the domain type whose constraints are being checked
- `extra`: Pointer to cached DomainIOData structure for performance optimization
- `mcxt`: Memory context for allocations (defaults to CurrentMemoryContext if NULL)
- `escontext`: Error context node for soft error handling (NULL for hard errors)

## Dependencies
- Functions called/Symbols referenced:
  - DomainIOData (data structure)
  - domain_state_setup
  - domain_check_input
  - SOFT_ERROR_OCCURRED
- Called from (representative examples):
  - domain_check
  - domain_check_safe

## Notes and Other Information
- This is a static internal function not exposed outside domains.c
- Implements caching optimization to avoid repeated domain state setup for the same domain type
- Returns `false` only when soft error mode is used and an error occurs
- The function handles memory context management, defaulting to CurrentMemoryContext if none specified
- Critical part of PostgreSQL's domain constraint enforcement mechanism
- Located at src/backend/utils/adt/domains.c:371-406