# xl_invalidations

## Location
src/include/storage/standbydefs.h: 63 - 70

## Overview
A WAL record structure that carries cache invalidation messages to standby servers, primarily used for transactions without assigned XIDs that still need to propagate invalidation events.

## Definition


## Detailed Description
The  structure is a specialized WAL record format used in PostgreSQL's standby recovery system to propagate cache invalidation messages from the primary to standby servers. This structure is particularly important for transactions that don't have assigned transaction IDs (XIDs) but still need to invalidate cached data structures on standby servers.

When operations modify system catalogs or other cached structures without being part of a regular transaction (such as certain DDL operations or system maintenance tasks), they still need to ensure that standby servers invalidate their corresponding caches. The  record packages these invalidation messages along with database and tablespace context information.

During WAL replay on standby servers, this record is processed by  which calls  to apply the invalidations locally, ensuring cache coherency across the cluster.

## Parameters / Member Variables
- : Database ID (MyDatabaseId) where the invalidations originated
- : Tablespace ID (MyDatabaseTableSpace) associated with the invalidations  
- : Boolean flag indicating whether relation cache initialization files need to be invalidated
- : Number of shared invalidation messages contained in the msgs array
- : Flexible array containing  structures with the actual invalidation data

## Dependencies
- Functions called/Symbols referenced:
  - SharedInvalidationMessage
  - FLEXIBLE_ARRAY_MEMBER
  - Oid
- Called from (representative examples):
  - LogStandbyInvalidations
  - standby_redo
  - standby_desc

## Notes and Other Information
- This record type is specifically designed for transactions without assigned XIDs that still need to propagate invalidation messages
- The  macro calculates the structure size without the flexible array member
- SharedInvalidationMessage is a union type that can represent different kinds of invalidation messages (catalog cache, relation cache, etc.)
- The relcacheInitFileInval flag triggers invalidation of the relation cache initialization files, forcing a complete reload
- Database and tablespace IDs provide context for applying invalidations in the correct scope on standby servers
- Unlike other standby WAL records, invalidation records are not typically marked as unimportant since cache consistency is crucial