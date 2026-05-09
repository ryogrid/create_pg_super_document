# PostgreSQL SSI Documentation Outline

## Part 1: Architectural Overview
### Chapter 1: SSI Fundamentals
- What is Serializable Snapshot Isolation
- How it differs from Snapshot Isolation
- The dangerous structure detection algorithm
- Conflict graph mechanics

### Chapter 2: Core Data Structures
- SERIALIZABLEXACT: Transaction state object
- PREDICATELOCKTARGETTAG: Lock target identification
- PREDICATELOCK: Individual lock objects
- RWConflictData: Conflict graph edges
- Shared memory layout and organization

## Part 2: Predicate Lock Subsystem
### Chapter 3: Lock Acquisition
- Lock granularity (relation/page/tuple)
- Tuple-level locking in heap scans
- Index-specific locking strategies
- Lock promotion and coalescing heuristics
- Per-transaction lock tracking

### Chapter 4: Lock Transfer & Maintenance
- Page splits and combines
- Index rebuilds and vacuum
- Lock transfer between targets
- Memory pressure handling

## Part 3: Conflict Detection & Resolution
### Chapter 5: Read-Write Conflict Detection
- CheckForSerializableConflictOut: Detecting conflicts on reads
- CheckForSerializableConflictIn: Detecting conflicts on writes
- MVCC integration for visibility checks
- Summary conflicts for old transactions

### Chapter 6: Dangerous Structure Detection
- The dangerous structure pattern (Tin-Tpivot-Tout)
- OnConflict_CheckForSerializationFailure algorithm
- Cycle detection in conflict graphs
- Decision logic for which transaction to abort

## Part 4: Transaction Lifecycle
### Chapter 7: Snapshot Acquisition & Registration
- GetSerializableTransactionSnapshot entry point
- Creating SERIALIZABLEXACT objects
- Global xmin tracking
- Read-only optimization eligibility

### Chapter 8: Commit-Time Validation
- PreCommit_CheckForSerializationFailure
- Final dangerous structure checking
- Serializability violation detection
- Error propagation

### Chapter 9: Cleanup & Summarization
- ReleasePredicateLocks at transaction end
- SummarizeOldestCommittedSxact to SLRU
- SerialAdd: persisting commit information
- Memory reclamation strategies

## Part 5: Advanced Topics
### Chapter 10: Read-Only Optimization
- Safe snapshot detection
- GetSafeSnapshot for DEFERRABLE transactions
- possibleUnsafeConflicts tracking
- RO_SAFE vs RO_UNSAFE states

### Chapter 11: Two-Phase Commit Integration
- Predicate lock persistence for prepared transactions
- Recovery from WAL records
- predicatelock_twophase_recover
- Cross-recovery conflict handling

### Chapter 12: Concurrency & Synchronization
- LWLock strategy (SerializableXactHashLock, etc.)
- Partition locking for scalability
- Parallel query worker support
- Lock ordering and deadlock prevention

### Chapter 13: Observability & Tools
- pg_locks view integration
- GetPredicateLockStatusData for monitoring
- Debug macros and error messages
- Conflict reporting to applications

## Implementation Catalogs (Stage 3)

### Catalog A: Function Reference
- All 50+ public and internal predicate functions
- Signatures, roles, and call sites
- Synchronization requirements
- Error conditions

### Catalog B: Data Structure Reference
- All 9+ key data structures
- Field descriptions and usage
- Memory layout and alignment
- Access patterns

### Catalog C: Lock & Synchronization Reference
- LWLock table (7 major locks)
- Hash table organization
- Partition strategies
- Lock ordering rules

### Catalog D: Critical Flow Diagrams
- Transaction lifecycle state machine
- Conflict detection flowchart
- Dangerous structure detection algorithm
- Cleanup and summarization pipeline

### Catalog E: Examples & Scenarios
- Simple serialization failure scenario
- Complex cycle with multiple transactions
- Read-only transaction optimization
- 2PC with conflicts

### Catalog F: Performance & Tuning
- GUC parameters (max_predicate_locks_*)
- Memory pressure and promotion
- Lock contention mitigation
- Query plan impact on locking
