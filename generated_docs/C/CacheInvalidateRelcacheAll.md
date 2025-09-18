# CacheInvalidateRelcacheAll

## Location
src/backend/utils/cache/inval.c: 1387 - 1398

## Overview
Registers invalidation of the entire relcache at the end of the current command, used when operations may affect a large number of tables.

## Definition


## Detailed Description
CacheInvalidateRelcacheAll invalidates the entire relation cache (relcache) for all relations in the current database. This is a heavyweight operation used in situations where changes may affect a large number of tables and it's more efficient to invalidate everything rather than try to identify and invalidate individual relations. The primary use case is with publication operations, where changes to publications may affect the caching behavior of many tables.

The function works by registering a special invalidation message with InvalidOid for both database and relation, which signals the invalidation system to flush all relcache entries.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - PrepareInvalidationState
  - RegisterRelcacheInvalidation
- Called from (representative examples):
  - CreatePublication
  - AlterPublicationOptions
  - InvalidatePublicationRels
  - RemovePublicationById

## Notes and Other Information
- This is the most aggressive form of relcache invalidation, affecting all relations
- It's primarily used in publication-related operations where the scope of affected relations is difficult to determine precisely
- The InvalidOid parameters to RegisterRelcacheInvalidation signal a global invalidation
- Should be used sparingly due to performance implications of rebuilding all relation cache entries
- The invalidation is deferred until command end to maintain consistency during complex operations