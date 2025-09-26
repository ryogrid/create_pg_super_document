# RelationData

## Location
src/include/utils/rel.h: 55 - 256

## Overview
RelationData is the core structure that represents a relation cache entry in PostgreSQL, containing all metadata and cached information about a table, index, or other relation object.

## Definition


## Detailed Description
RelationData is the fundamental structure used by PostgreSQL's relation cache (relcache) system. It serves as a comprehensive cache entry that holds all the metadata and derived information about a database relation (table, index, view, etc.). The structure is designed to minimize expensive catalog lookups by caching frequently accessed relation information in memory.

The structure includes several categories of information:
- Basic relation identification and physical storage details
- Transaction state tracking for MVCC and subtransaction handling
- Core relation metadata from system catalogs
- Derived information like indexes, constraints, and statistics
- Access method specific data and function pointers
- Memory management contexts for various cached data

This cache entry is reference-counted and managed by the relcache system, which handles invalidation and reconstruction when the underlying relation metadata changes.

## Parameters / Member Variables
Core identification and storage:
- : Physical identifier for the relation's storage files
- : Cached storage manager relation handle for file operations
- : Reference count for memory management
- : Object identifier (OID) of the relation

State and validity tracking:
- : Process number of owning backend for temporary relations
- : Whether this is a temporary relation of the current session
- : Whether this entry should never be removed from cache
- : Whether the relcache entry contains valid data
- : Whether the cached index list is valid
- : Whether the cached statistics list is valid

Transaction tracking:
- : Subtransaction ID when relation was created
- : Subtransaction ID of most recent relfilenumber change
- : Subtransaction ID of first rd_locator change
- : Subtransaction ID when relation was dropped

Core metadata:
- : Pointer to the pg_class catalog tuple for this relation
- : Tuple descriptor defining the relation's column structure
- : Lock manager information for relation-level locking
- : Query rewrite rules associated with the relation
- : Trigger definitions and metadata

Partitioning support:
- : Partition key specification for partitioned tables
- : Partition descriptor with child partition information
- : Partition constraint expressions

Index and constraint information:
- : List of OIDs of all indexes on the relation
- : OID of the primary key index
- : OID of the replica identity index
- : Bitmap of columns referenced by foreign keys

Access methods:
- : Table access method function pointers
- : Index access method function pointers
- : OID of the access method handler function

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocator, SMgrRelation, ProcNumber
  - Form_pg_class, TupleDesc, LockInfoData
  - PartitionKey, PartitionDesc, TriggerDesc
  - TableAmRoutine, IndexAmRoutine, FdwRoutine
  - Various list and memory context types
- Called from (representative examples):
  - AllocateRelationDesc
  - RelationBuildLocalRelation
  - load_relcache_init_file
  - XLogReadBufferExtended

## Notes and Other Information
- This structure is at the heart of PostgreSQL's relcache system and is one of the most important data structures for relation metadata management
- The structure contains multiple memory contexts for managing different types of cached data with appropriate lifetimes
- Transaction and subtransaction tracking fields are critical for MVCC correctness and WAL logging decisions
- The structure is designed to be extensible, with access method specific data stored in rd_amcache
- Foreign table support is provided through rd_fdwroutine function pointers
- Statistics collection integration is provided through pgstat_info
- The structure balances memory usage with performance by lazy-loading expensive derived information