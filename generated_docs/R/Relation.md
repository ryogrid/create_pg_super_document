# Relation

## Location
[src/include/utils/relcache.h:27-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/relcache.h#L27-L34)

## Overview
Relation is a typedef that represents a pointer to a RelationData struct, serving as the primary data structure for representing database relations (tables, indexes, sequences, etc.) in PostgreSQL's relation cache system.

## Definition
```c
typedef struct RelationData *Relation;
```

## Detailed Description
The Relation type is a fundamental abstraction in PostgreSQL that provides a unified interface for accessing and manipulating database relations. It is essentially a pointer to a RelationData structure, which contains comprehensive metadata about a database relation including its physical location, schema information, access methods, indexing details, and various cached data for performance optimization.

The RelationData structure is the core of PostgreSQL's relation cache (relcache) system, which maintains in-memory representations of relation metadata to avoid repeated system catalog lookups. This caching mechanism is crucial for performance, as relation metadata is frequently accessed during query execution.

The structure contains extensive information organized into several categories:
- Physical storage information (file location, storage manager)
- Reference counting and validity flags for cache management
- Transaction and subtransaction tracking for MVCC support
- Schema information (tuple descriptor, relation tuple from pg_class)
- Security and access control (locks, rules, triggers, row security)
- Indexing and partitioning metadata
- Foreign key relationships and constraints
- Statistics and publication information
- Access method specific data for both table and index access methods
- Foreign table support for foreign data wrappers

## Parameters / Member Variables
The RelationData struct contains numerous fields organized by functionality:

**Core Identification and Storage:**
- `rd_locator`: Physical file location identifier for the relation
- `rd_smgr`: Cached storage manager file handle, or NULL if not opened
- `rd_id`: The relation's object identifier (OID)
- `rd_rel`: The pg_class tuple containing relation metadata
- `rd_att`: Tuple descriptor defining the relation's schema

**Cache Management:**
- `rd_refcnt`: Reference count for memory management
- `rd_backend`: Process number if this is a temporary relation
- `rd_islocaltemp`: Boolean indicating if relation is a temp rel of current session
- `rd_isnailed`: Boolean indicating if relation is permanently cached
- `rd_isvalid`: Boolean indicating if cache entry is valid
- `rd_indexvalid`: Boolean indicating if index list cache is valid
- `rd_statvalid`: Boolean indicating if statistics list cache is valid

**Transaction Tracking:**
- `rd_createSubid`: Subtransaction ID where relation was created
- `rd_newRelfilelocatorSubid`: Highest subtrans ID for recent file location changes
- `rd_firstRelfilelocatorSubid`: Highest subtrans ID for any file location changes
- `rd_droppedSubid`: Subtransaction ID where relation was dropped

**Access Control and Rules:**
- `rd_lockInfo`: Lock manager information for relation locking
- `rd_rules`: Rewrite rules attached to the relation
- `rd_rulescxt`: Private memory context for rules
- `trigdesc`: Trigger descriptor, or NULL if no triggers
- `rd_rsdesc`: Row security policies, or NULL if none

**Relationships and Constraints:**
- `rd_fkeylist`: List of foreign key constraint information
- `rd_fkeyvalid`: Boolean indicating if foreign key list is valid

**Partitioning Support:**
- `rd_partkey`: Partition key information for partitioned tables
- `rd_partkeycxt`: Private memory context for partition key
- `rd_partdesc`: Partition descriptor with all partitions
- `rd_partdesc_nodetached`: Partition descriptor excluding detached partitions
- `rd_pdcxt`, `rd_pddcxt`: Memory contexts for partition descriptors
- `rd_partdesc_nodetached_xmin`: Transaction ID for partition descriptor validation
- `rd_partcheck`: Partition CHECK constraint expressions
- `rd_partcheckvalid`: Boolean indicating if partition check is valid
- `rd_partcheckcxt`: Memory context for partition check

**Indexing:**
- `rd_indexlist`: List of OIDs of indexes on this relation
- `rd_pkindex`: OID of primary key index, if any
- `rd_ispkdeferrable`: Boolean indicating if primary key is deferrable
- `rd_replidindex`: OID of replica identity index, if any

**Statistics:**
- `rd_statlist`: List of OIDs of extended statistics objects

**Attribute Bitmaps:**
- `rd_attrsvalid`: Boolean indicating if attribute bitmaps are valid
- `rd_keyattr`: Columns that can be referenced by foreign keys
- `rd_pkattr`: Columns included in primary key
- `rd_idattr`: Columns included in replica identity index
- `rd_hotblockingattr`: Columns that block HOT updates
- `rd_summarizedattr`: Columns indexed by summarizing indexes

**Publication and Replication:**
- `rd_pubdesc`: Publication descriptor for logical replication

**Options and Access Methods:**
- `rd_options`: Parsed relation options from pg_class.reloptions
- `rd_amhandler`: OID of access method handler function
- `rd_tableam`: Table access method API structure

**Index-Specific Information:**
- `rd_index`: pg_index tuple for index relations
- `rd_indextuple`: Complete pg_index tuple data
- `rd_indexcxt`: Private memory context for index data
- `rd_indam`: Index access method API structure
- `rd_opfamily`: Operator family OIDs for each index column
- `rd_opcintype`: Input data types for operator classes
- `rd_support`: Support procedure OIDs
- `rd_supportinfo`: Lookup information for support procedures
- `rd_indoption`: Per-column access method specific flags
- `rd_indexprs`: Index expression trees
- `rd_indpred`: Index predicate expression tree
- `rd_exclops`: Exclusion constraint operator OIDs
- `rd_exclprocs`: Exclusion constraint procedure OIDs
- `rd_exclstrats`: Exclusion constraint strategy numbers
- `rd_indcollation`: Index collation OIDs
- `rd_opcoptions`: Parsed operator class options

**Extension Points:**
- `rd_amcache`: Cache area for access method specific data
- `rd_fdwroutine`: Foreign data wrapper function pointers

**Special Features:**
- `rd_toastoid`: OID of associated TOAST table for large attribute storage
- `pgstat_enabled`: Boolean indicating if statistics should be collected
- `pgstat_info`: Statistics collection area

## Dependencies
- Functions called/Symbols referenced:
  - [RelationData](RelationData.md) (the underlying struct)
  - Various system catalog structures (Form_pg_class, Form_pg_index)
  - Memory management structures (MemoryContext)
  - Access method structures (TableAmRoutine, IndexAmRoutine)
  - Lock management structures (LockInfoData)

- Called from (representative examples):
  - [AllocateRelationDesc](../A/AllocateRelationDesc.md) (src/backend/utils/cache/relcache.c:421)
  - [RelationBuildLocalRelation](RelationBuildLocalRelation.md) (src/backend/utils/cache/relcache.c:3593)
  - [formrdesc](../f/formrdesc.md) (src/backend/utils/cache/relcache.c:1886)
  - [XLogReadBufferExtended](../X/XLogReadBufferExtended.md) (src/backend/access/transam/xlogutils.c:561)

## Notes and Other Information
- The Relation typedef is used throughout PostgreSQL to pass relation metadata between functions
- The RelationData structure is one of the largest and most complex data structures in PostgreSQL, reflecting the complexity of managing database relations
- The relation cache is critical for performance as it avoids repeated expensive system catalog lookups
- Memory management is carefully handled with reference counting and specific memory contexts
- The structure supports both regular heap tables and various specialized relation types (indexes, sequences, foreign tables, etc.)
- Transaction tracking fields are essential for MVCC (Multi-Version Concurrency Control) correctness
- The extensive index-related fields reflect PostgreSQL's sophisticated indexing capabilities
- Partitioning support includes complex logic for handling detached partitions during DDL operations