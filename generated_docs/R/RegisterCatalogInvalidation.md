# RegisterCatalogInvalidation

## Location
[src/backend/utils/cache/inval.c:559-570](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L559-L570)

## Overview
Registers an invalidation event for all catcache entries from a specific catalog, marking them for invalidation during the current command.

## Definition


## Detailed Description
RegisterCatalogInvalidation is a static function that registers a catalog invalidation message for a specific catalog. It works by adding the invalidation message to the current command's invalidation message queue. This function is part of PostgreSQL's cache invalidation system, ensuring that catalog cache entries are properly invalidated when catalog tables are modified.

The function operates at the command level, meaning invalidations registered during a command will be processed when the command completes. This helps maintain cache consistency by ensuring that stale catalog cache entries are removed after catalog modifications.

## Parameters / Member Variables
- : Database OID where the catalog resides (InvalidOid for shared catalogs)
- : OID of the catalog table being invalidated

## Dependencies
- Functions called/Symbols referenced:
  - [AddCatalogInvalidationMessage](../A/AddCatalogInvalidationMessage.md)
- Called from (representative examples):
  - [CacheInvalidateCatalog](../C/CacheInvalidateCatalog.md)

## Notes and Other Information
- This is a static function internal to the invalidation system
- Part of the transaction-level invalidation state management
- Works in conjunction with the command counter mechanism to ensure proper timing of invalidations
- The actual invalidation processing happens later when the command completes