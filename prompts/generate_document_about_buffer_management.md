# PostgreSQL Buffer Management Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive technical documentation for PostgreSQL's **Buffer Management (Shared Buffers)** system, covering the complete lifecycle from buffer pool initialization through page lookup, pin/unpin protocol, content locking, **page-level concurrency control (lock types, lock ordering, per-operation lock acquisition patterns)**, dirty page tracking, page layout and types, storage manager interaction, write-back strategies (checkpointing, background writer), WAL-before-data guarantees, and OS page cache considerations.

## Output Directory
All generated artifacts — intermediate files (architecture_map.json, key_symbols.txt, etc.), component files, diagrams, and final documentation modules — **must** be written under the following directory:

```
topic_specific_generated_docs/about_buffer_management/
```

Create this directory at the start of Stage 1 if it does not already exist. Use subdirectories as needed to organize outputs by stage:

```
topic_specific_generated_docs/about_buffer_management/
├── stage1/                          # Architecture analysis outputs
│   ├── architecture_map.json
│   ├── key_symbols.txt
│   └── initial_outline.md
├── stage2/                          # Detailed documentation components
│   ├── component_*.md
│   └── diagrams/
│       └── *.mermaid
├── final/                           # Integrated final documentation
│   ├── index.md
│   ├── 01_executive_summary.md
│   ├── ...
│   ├── 15_deep_dives.md
│   ├── appendix_*.md
│   ├── buffer_mgmt_quick_reference.md
│   ├── buffer_mgmt_api_reference.md
│   └── quality_report.md
└── diagrams/                        # Final consolidated diagrams
    └── *.mermaid
```

**All file paths referenced between stages (e.g., Stage 2 reading Stage 1 outputs) must use paths relative to `topic_specific_generated_docs/about_buffer_management/`.**

## Available Resources

### MCP Server Capabilities
You have access to a specialized MCP server with these functions:
- `pg_symbol_overview(symbol)` - Get concise overview (low context usage)
- `pg_symbol_document(symbol)` - Get detailed documentation
- `pg_symbol_source(symbol)` - Retrieve source code for a symbol
- `pg_references_from(symbol)` - Get symbols referenced by this symbol
- `pg_references_to(symbol)` - Get symbols that reference this symbol

### Local Source Code (PostgreSQL `src/` directory)
The PostgreSQL source tree is available locally at `./src/`. This is a direct copy of the upstream `src/` directory and should be actively referenced throughout all stages. Key directories for Buffer Management documentation:

| Directory | Contents |
|---|---|
| `src/backend/storage/buffer/` | Buffer manager core — `bufmgr.c` (pin/unpin, ReadBuffer, page fetch), `buf_init.c` (pool initialization), `buf_table.c` (buffer hash table lookup), `freelist.c` (free list and clock sweep replacement), `localbuf.c` (local buffers for temp tables) |
| `src/backend/storage/smgr/` | Storage manager — `smgr.c` (storage manager dispatcher), `md.c` (magnetic disk / file-based storage implementation) |
| `src/backend/storage/page/` | Page-level utilities — `bufpage.c` (page initialization, item pointer management, page layout operations) |
| `src/backend/storage/lmgr/` | Lock manager — `lwlock.c` (lightweight locks including buffer content locks and I/O locks), `lmgr.c` (lock manager interface), `lock.c` (heavyweight lock implementation) |
| `src/backend/storage/ipc/` | Shared memory infrastructure — `shmem.c` (shared memory allocation), `ipci.c` (IPC initialization) |
| `src/backend/access/heap/` | Heap access methods — `heapam.c` (heap tuple access using buffer manager), `hio.c` (heap I/O: buffer allocation for heap inserts) |
| `src/backend/access/nbtree/` | B-tree access — `nbtree.c`, `nbtinsert.c`, `nbtpage.c` (B-tree page operations via buffer manager) |
| `src/backend/access/transam/` | Transaction and WAL — `xlog.c` (WAL write and flush), `xloginsert.c` (WAL record construction), `slru.c` (Simple LRU for CLOG etc.) |
| `src/backend/postmaster/` | Background processes — `bgwriter.c` (background writer), `checkpointer.c` (checkpoint process), `walwriter.c` (WAL writer) |
| `src/backend/commands/` | User-facing commands — `vacuum.c` (VACUUM's interaction with buffers) |
| `src/backend/catalog/` | System catalog access — `catalog.c` (catalog page access patterns) |
| `src/include/storage/` | Key headers — `buf.h` (Buffer typedef), `buf_internals.h` (BufferDesc, buffer tag, state flags), `bufmgr.h` (public buffer manager API), `bufpage.h` (PageHeaderData, page layout macros), `smgr.h` (SMgrRelation), `lwlock.h` (LWLock definitions), `lock.h` (heavyweight lock types), `shmem.h`, `block.h` (BlockNumber), `relfilenode.h` / `relfilelocator.h` (RelFileNode/RelFileLocator), `condition_variable.h` (ConditionVariable used by buffer wait) |
| `src/include/access/` | Access method headers — `heapam.h`, `xlog.h`, `xlogdefs.h` (XLogRecPtr, LSN types) |
| `src/include/common/` | `relpath.h` (relation file path utilities) |

**Usage guidelines for source code**:
- **Prefer direct source reading** over MCP `pg_symbol_source()` when exploring file-level structure, neighboring functions, or header definitions. Use `cat`, `grep`, `find`, and `head`/`tail` to navigate the tree.
- **Use MCP tools** for targeted symbol lookups, cross-reference analysis, and pre-indexed documentation.
- When documenting a function, always verify its actual signature and logic against the local source (`./src/...`) as the ground truth.
- Use `grep -rn` to discover call sites, `#define` constants, and struct definitions that MCP may not fully index.
- When quoting source code in documentation, include the relative file path (e.g., `src/backend/storage/buffer/bufmgr.c:456`) for traceability.

### Available Subagents
1. **architecture-analyzer** - Analyzes codebase structure and dependencies
2. **detail-documenter** - Creates detailed technical documentation
3. **integration-optimizer** - Integrates and optimizes final documentation

---

## Execution Plan

### Stage 1: Architecture Analysis
Invoke the architecture-analyzer subagent with the following instruction:

```
Analyze the PostgreSQL Buffer Management (Shared Buffers) architecture.

Use BOTH the MCP server tools AND the local source tree (`./src/`) for analysis.

**Source exploration strategy for this stage**:
- Start by scanning key directories to identify relevant files:
  - `find ./src/backend/storage/buffer/ -name '*.c'`
  - `find ./src/backend/storage/smgr/ -name '*.c'`
  - `find ./src/backend/storage/page/ -name '*.c'`
  - `find ./src/backend/storage/lmgr/ -name '*.c' | head -10`
  - `find ./src/backend/storage/ipc/ -name '*.c' | head -10`
  - `find ./src/backend/postmaster/ -name '*.c' | grep -E 'bgwriter|checkpointer|walwriter'`
- Use `grep -rn 'FunctionName' ./src/` to trace call chains and discover symbols the MCP index might miss.
- Read key header files (`src/include/storage/buf_internals.h`, `src/include/storage/bufmgr.h`, `src/include/storage/bufpage.h`, `src/include/storage/smgr.h`) to understand data structure definitions.
- Cross-validate MCP `pg_references_from()` / `pg_references_to()` results against `grep` results in the source tree.

Build a comprehensive dependency map with depth 5 traversal. Focus on:

1. Buffer pool architecture and initialization
   - Shared buffer pool layout in shared memory (NBuffers, BufferDescriptors[], BufferBlocks[])
   - Buffer pool initialization (InitBufferPool, buf_init.c)
   - Shared memory allocation for buffer structures (ShmemInitStruct, BufferShmemSize)
   - Relationship between buffer IDs, buffer descriptors, and buffer pages

2. Buffer descriptor and metadata
   - BufferDesc structure: tag (RelFileLocator + ForkNumber + BlockNumber), state (flags + refcount + usagecount), content_lock, freeNext
   - Buffer tag structure (BufferTag): relfilelocator, forkNum, blockNum — how pages are uniquely identified
   - Buffer state flags: BM_DIRTY, BM_VALID, BM_TAG_VALID, BM_IO_IN_PROGRESS, BM_IO_ERROR, BM_JUST_DIRTIED, BM_LOCKED
   - Atomic state word encoding (refcount, usage_count, flags packed into uint32)

3. Buffer lookup and hash table
   - Buffer hash table (buf_table.c): BufTableLookup, BufTableInsert, BufTableDelete
   - Partition-based locking of the buffer hash table (NUM_BUFFER_PARTITIONS)
   - Lookup path: tag → hash → partition lock → descriptor

4. Buffer access protocol (pin/unpin and content locks)
   - ReadBuffer / ReadBufferExtended — the primary entry point for page access
   - BufferAlloc — finding or allocating a buffer for a given page
   - PinBuffer / UnpinBuffer — reference counting protocol
   - LockBuffer / LockBufferForCleanup — content lock acquisition (shared/exclusive)
   - Buffer access rules: must pin before access, must lock for read/write, unlock before unpin
   - Local pin count tracking per backend (PrivateRefCount)

5. Page-level concurrency control (buffer content locks and beyond)
   - Buffer content locks (LWLock per buffer): shared (read) vs exclusive (write) semantics
   - LWLock implementation for buffer content locks (LWLockAcquire, LWLockRelease, LWLockConditionalAcquire on BufferDescriptor->content_lock)
   - Relationship between pin and content lock: pin prevents eviction, content lock controls read/write access
   - Concurrent read access: multiple backends holding BUFFER_LOCK_SHARE simultaneously
   - Exclusive write access: BUFFER_LOCK_EXCLUSIVE for page modification
   - LockBuffer() API and its mapping to LWLock operations
   - LockBufferForCleanup(): waiting until pin count drops to 1 for exclusive + sole-access (used by VACUUM, HOT pruning)
   - ConditionalLockBuffer(): non-blocking lock attempt and its use cases
   - Buffer I/O lock (BM_IO_IN_PROGRESS flag): serializing concurrent I/O on the same buffer (StartBufferIO, TerminateBufferIO)
   - Buffer header spinlock (buf_hdr_lock via atomic operations): protecting descriptor state field updates (refcount, usage_count, flags)
   - Lock ordering conventions: hash partition lock → buffer header lock → content lock → I/O lock (deadlock avoidance)
   - Lock acquisition patterns by operation type:
     - Sequential scan: pin → shared content lock → read → unlock → unpin
     - Heap insert/update: pin → exclusive content lock → modify → WAL insert → mark dirty → unlock → unpin
     - Index scan: pin → shared lock → examine → conditional upgrade or re-lock for modification
     - VACUUM: LockBufferForCleanup → exclusive cleanup access → prune/defragment → unlock → unpin
   - Interaction with heavyweight (relation-level) locks: how relation locks and page-level locks cooperate
   - Buffer lock contention scenarios and diagnostics (wait event types: BufferPin, BufferContent, BufferIO)

6. Buffer replacement policy (clock sweep)
   - Clock sweep algorithm (StrategyGetBuffer in freelist.c)
   - Usage count mechanism (BUF_USAGECOUNT_ONE, MAX_USAGE_COUNT)
   - Free list for unused buffers
   - Ring buffer strategy for bulk operations (GetBulkStrategy, BAS_BULKREAD, BAS_BULKWRITE, BAS_VACUUM)
   - Strategy objects (BufferAccessStrategy) and their role in limiting buffer pool pollution

7. Page layout and page types
   - PageHeaderData structure (pd_lsn, pd_checksum, pd_flags, pd_lower, pd_upper, pd_special, pd_pagesize_version)
   - Item pointers (ItemIdData / line pointers) and tuple storage within a page
   - Free space management within a page (pd_lower → pd_upper gap)
   - Page types by fork:
     - Main fork pages (heap pages, index pages — B-tree, GiST, GIN, etc.)
     - FSM (Free Space Map) fork pages
     - VM (Visibility Map) fork pages
     - Init fork
   - Special space at end of page (pd_special) used by index access methods
   - Page initialization (PageInit) and validity checks (PageIsVerified, PageIsNew)

8. Dirty buffer management and write-back
   - MarkBufferDirty — marking a buffer as dirty after modification
   - FlushBuffer / SyncOneBuffer — writing dirty pages to storage
   - Background writer (bgwriter.c): periodic dirty buffer write-back, BgBufferSync
   - Checkpoint process (checkpointer.c): BufferSync — flushing all dirty buffers at checkpoint
   - Write-back ordering and scheduling (WritebackContext, IssuePendingWritebacks)

9. WAL-before-data guarantee and LSN management
   - Write-ahead logging rule: WAL record must be flushed before dirty data page is written
   - Page LSN (pd_lsn in PageHeaderData): how buffer manager uses LSN to enforce WAL ordering
   - XLogFlush before buffer write-back (in FlushBuffer)
   - Relationship between XLogRecPtr (LSN) and page dirty/flush lifecycle
   - Full-page writes (FPW) after checkpoint — protecting against torn pages

10. Storage manager layer (smgr)
   - SMgrRelation structure and smgr API (smgropen, smgrread, smgrwrite, smgrextend, smgrcreate, smgrunlink)
   - md.c (magnetic disk implementation): MdfdVec, segment-based file layout
   - Relation fork abstraction (ForkNumber: MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM, INIT_FORKNUM)
   - File descriptor management and VFD (Virtual File Descriptor) layer (fd.c)
   - Relation file naming and path resolution (relpath.c / relfilelocator)

11. Data movement: memory ↔ kernel page cache ↔ persistent storage
    - Two-tier caching: PostgreSQL shared buffers → OS page cache → disk
    - Double buffering problem and mitigation strategies (O_DIRECT considerations)
    - fsync semantics: when data is guaranteed to reach persistent storage
    - fsync methods (fsync, fdatasync, open_datasync, open_sync) and GUC configuration (wal_sync_method)
    - Checkpoint as the durability boundary: dirty pages written + WAL flushed + fsync
    - Crash recovery perspective: which pages need re-application of WAL

12. Local buffers for temporary tables
    - Local buffer pool (localbuf.c): separate per-backend, not in shared memory
    - LocalBufferAlloc, GetLocalBufferStorage
    - No need for locking or WAL logging for temp table pages
    - Different lifecycle and cleanup semantics

13. Buffer manager integration with access methods
    - How heap access methods use ReadBuffer / ReleaseBuffer
    - How B-tree operations acquire and release buffer pins
    - How VACUUM interacts with the buffer pool (buffer access strategy, ring buffers)
    - Buffer prefetching (PrefetchBuffer) for sequential scan optimization
    - Relation extension and new page allocation (ReadBufferExtended with P_NEW)

Generate (all files under `topic_specific_generated_docs/about_buffer_management/stage1/`):
- architecture_map.json with importance scores (0.0–1.0) for each symbol
- key_symbols.txt (top 30 symbols ranked by importance)
- initial_outline.md with suggested documentation structure
```

**Expected Output Check**: Verify architecture_map.json contains at least 50 symbols and identifies 6+ critical paths (e.g., ReadBuffer path, buffer replacement path, dirty buffer write-back path, checkpoint flush path, page lookup path, storage manager read/write path, page-level lock acquisition path).

---

### Stage 2: Detailed Documentation Generation
After Stage 1 completes, invoke the detail-documenter subagent:

```
Using the architecture analysis from Stage 1, create detailed documentation for the PostgreSQL Buffer Management system.

**Source code usage for this stage**:
- For every Tier 1 symbol (importance > 0.8), read the full function implementation from `./src/` and annotate key logic steps.
- When documenting the buffer access protocol, read `src/backend/storage/buffer/bufmgr.c` end-to-end — this is the single most critical file for buffer management understanding.
- When documenting buffer replacement, read `src/backend/storage/buffer/freelist.c` focusing on `StrategyGetBuffer()` and `GetBufferFromRing()`.
- When documenting the hash table, read `src/backend/storage/buffer/buf_table.c` for the partitioned hash lookup.
- When documenting buffer initialization, read `src/backend/storage/buffer/buf_init.c`.
- When documenting page layout, read `src/include/storage/bufpage.h` for `PageHeaderData` and item pointer definitions, and `src/backend/storage/page/bufpage.c` for operations.
- When documenting the storage manager, read `src/backend/storage/smgr/smgr.c` and `src/backend/storage/smgr/md.c`.
- When documenting background writer and checkpointing, read `src/backend/postmaster/bgwriter.c` and `src/backend/postmaster/checkpointer.c`.
- When documenting WAL interaction, read relevant sections of `src/backend/access/transam/xlog.c` focusing on `XLogFlush()` and full-page write logic.
- When documenting page-level concurrency control, read `src/backend/storage/buffer/bufmgr.c` focusing on `LockBuffer()`, `LockBufferForCleanup()`, `ConditionalLockBuffer()`, `StartBufferIO()`, `TerminateBufferIO()`, `WaitIO()`. Also read `src/backend/storage/lmgr/lwlock.c` for the LWLock implementation underlying buffer content locks. Read `src/include/storage/buf_internals.h` for the buffer state atomic word encoding (refcount + usagecount + flags). Use `grep -rn 'LockBuffer\|BUFFER_LOCK_SHARE\|BUFFER_LOCK_EXCLUSIVE\|LockBufferForCleanup' ./src/backend/access/` to discover lock acquisition patterns across heap, B-tree, and other access methods.
- For data structure documentation, directly quote struct definitions from header files (e.g., `BufferDesc` from `src/include/storage/buf_internals.h`, `PageHeaderData` from `src/include/storage/bufpage.h`).
- Include file paths and line numbers in all source references for traceability.
- Use `grep -rn` to find all callers of key functions to document integration patterns accurately.

Input files (from `topic_specific_generated_docs/about_buffer_management/stage1/`):
- architecture_map.json
- key_symbols.txt
- initial_outline.md

Documentation Requirements:

1. For each symbol with importance > 0.8:
   - Complete API documentation (signature, parameters, return values)
   - Internal logic explanation with step-by-step walkthrough
   - Caller/callee relationships and integration patterns
   - Performance characteristics and concurrency implications
   - Key invariants and assumptions

2. For each symbol with importance 0.5–0.8:
   - API documentation (signature, brief description)
   - Role within the broader buffer management system
   - Key relationships to Tier 1 symbols

3. Required Diagrams (minimum 9):
   - Shared buffer pool memory layout (BufferDescriptors[], BufferBlocks[], hash table, free list)
   - ReadBuffer control flow diagram (tag lookup → hash table → hit/miss → allocation → I/O)
   - Pin/unpin and content lock protocol state diagram
   - Page-level concurrency control lock hierarchy diagram (hash partition lock → buffer header spinlock → content lock → I/O lock, showing lock ordering rules, acquisition/release sequences per operation type, and deadlock avoidance conventions)
   - Clock sweep buffer replacement algorithm flowchart
   - Page internal layout diagram (PageHeaderData, line pointers, tuples, special space)
   - Dirty buffer write-back pipeline (MarkBufferDirty → bgwriter/checkpoint → smgr → fsync)
   - Data flow through the storage stack (shared buffers → OS page cache → persistent storage) with WAL ordering
   - Buffer access strategy and ring buffer diagram (bulk read/write/vacuum strategies)

4. Special Focus Areas (dedicate extra depth):
   - Buffer access protocol: exhaustive walkthrough of ReadBuffer → BufferAlloc → I/O path, including every lock acquired and released
   - Pin/unpin semantics: reference counting, PrivateRefCount optimization, local vs shared refcount
   - Clock sweep details: usage count decrement, free list interaction, victim selection
   - Page layout internals: how tuples, line pointers, and free space are organized; differences between heap pages and index pages
   - WAL-before-data enforcement: exactly how and where LSN comparison prevents premature page write-back
   - Checkpoint mechanics: how BufferSync iterates dirty buffers, write ordering, fsync batching
   - Ring buffer strategies: how BAS_BULKREAD/BAS_BULKWRITE/BAS_VACUUM limit buffer pool pollution during bulk operations
   - Double buffering: the interaction between PostgreSQL's shared buffers and the OS page cache, trade-offs, and O_DIRECT considerations
   - Local buffers: how temp table pages bypass shared buffer pool entirely
   - Full-page writes: torn page protection after checkpoint, performance implications, wal_compression
   - Page-level concurrency control: complete taxonomy of lock types (content lock, I/O lock, buffer header spinlock, pin-based protection) with their granularity, duration, and interaction rules; step-by-step lock acquisition traces for representative operations (SELECT scan, INSERT, UPDATE, DELETE, VACUUM cleanup); LockBufferForCleanup's spin-wait-with-signal mechanism for sole-access acquisition; I/O lock protocol (BM_IO_IN_PROGRESS) preventing duplicate reads from disk; deadlock avoidance through strict lock ordering conventions; contention hotspots and monitoring via pg_stat_activity wait events (BufferPin, BufferContent, BufferIO)

5. Source code references:
   - For each major function, include the relevant source file path
   - Quote critical code sections (≤20 lines) with inline annotations
   - Note important #define constants and their values (NBuffers, BLCKSZ, NUM_BUFFER_PARTITIONS, etc.)

Generate component files organized by functional area (all files under `topic_specific_generated_docs/about_buffer_management/stage2/`):
- component_buffer_pool_architecture.md   (pool layout, initialization, shared memory, buffer descriptors)
- component_buffer_lookup_and_hashtable.md (buffer tag, hash table, partitioned locking)
- component_buffer_access_protocol.md     (ReadBuffer, pin/unpin, content locks, access rules)
- component_page_concurrency_control.md   (buffer content locks, I/O locks, header spinlock, lock ordering, per-operation lock traces, LockBufferForCleanup, contention diagnostics)
- component_buffer_replacement.md         (clock sweep, usage count, free list, ring buffer strategies)
- component_page_layout.md               (PageHeaderData, line pointers, page types, forks, special space)
- component_dirty_buffer_writeback.md     (MarkBufferDirty, bgwriter, checkpoint, write ordering)
- component_wal_integration.md            (WAL-before-data, LSN, full-page writes, crash recovery)
- component_storage_manager.md            (smgr, md.c, VFD, relation forks, file layout)
- component_data_movement.md              (shared buffers ↔ OS page cache ↔ disk, fsync, durability)
- component_local_buffers.md              (temp tables, local buffer pool, differences from shared buffers)
- component_access_method_integration.md  (heap, B-tree, VACUUM, prefetch, relation extension)
- diagrams/*.mermaid                      (under `topic_specific_generated_docs/about_buffer_management/stage2/diagrams/`)
```

**Expected Output Check**: Ensure all Tier 1 symbols (importance > 0.8) have detailed documentation with source references. Verify minimum 9 diagrams are generated.

---

### Stage 3: Integration and Optimization
After Stage 2 completes, invoke the integration-optimizer subagent:

```
Integrate all documentation components into a cohesive, professional technical document.

**Source code verification for this stage**:
- Before finalizing, spot-check at least 10 critical function signatures and struct definitions against `./src/` to ensure accuracy.
- Verify that all quoted code snippets in the documentation match the actual source.
- Confirm file paths referenced in the documentation are valid: `ls ./src/path/to/file.c`.
- If any discrepancies are found between MCP-sourced information and the local source tree, the local source tree is authoritative.

Input files (from `topic_specific_generated_docs/about_buffer_management/stage2/`):
- All component_*.md files from Stage 2
- All diagrams/*.mermaid files
- architecture_map.json for reference (from `topic_specific_generated_docs/about_buffer_management/stage1/`)

Integration Requirements:

1. Document Structure:
   - Executive Summary (1 page): Buffer management's role in PostgreSQL's I/O architecture, design philosophy (shared buffer pool as unified page cache), and key trade-offs (memory usage vs. I/O performance, double buffering)
   - Architecture Overview: System-wide perspective with main structural diagram showing how buffer manager connects access methods above and storage manager below
   - Core Components (organized by operational flow):
     a. Buffer Pool Architecture — shared memory layout, initialization, buffer descriptors and IDs
     b. Buffer Lookup and Hash Table — how pages are located in the pool via buffer tags
     c. Buffer Access Protocol — ReadBuffer, pin/unpin, content locking, backend interaction patterns
     d. Page-level Concurrency Control — lock types (content lock, I/O lock, header spinlock, pin), lock ordering and deadlock avoidance, per-operation lock acquisition patterns, contention diagnostics
     e. Buffer Replacement Policy — clock sweep, usage counts, free list, strategy objects
     f. Page Layout and Types — internal page structure, page types by fork, item pointer management
     g. Dirty Buffer Management and Write-back — marking dirty, background writer, checkpoint flush
     h. WAL Integration — write-ahead logging guarantee, LSN-based write ordering, full-page writes
     i. Storage Manager — smgr abstraction, md.c file-based implementation, VFD layer
     j. Data Movement and Durability — memory → OS cache → persistent storage, fsync semantics
     k. Local Buffers — temp table handling outside shared buffer pool
     l. Access Method Integration — how heap, index, and VACUUM operations use the buffer manager
   - Deep Dives: Complex topics including:
     - Double buffering problem and O_DIRECT considerations
     - Ring buffer strategies for bulk I/O operations
     - Full-page writes and torn page protection
     - Buffer manager interaction with crash recovery (WAL replay re-applying pages)
     - Relation extension protocol and race conditions
     - Buffer pool sizing considerations and performance implications
     - Checksum verification at read time (data_checksums)
     - Buffer lock contention analysis: common bottlenecks (hot pages, relation extension lock), monitoring with pg_stat_activity wait events, and tuning strategies
   - Appendices:
     - Symbol index (alphabetical, with source file locations)
     - Glossary of buffer management terms
     - Key data structure reference (BufferDesc, PageHeaderData, SMgrRelation, BufferTag, BufferAccessStrategy, etc.)
     - Key GUC parameters (shared_buffers, bgwriter_delay, bgwriter_lru_maxpages, checkpoint_completion_target, wal_sync_method, effective_io_concurrency, etc.)
     - Further reading (relevant PostgreSQL source files, README files in source tree, wiki pages)

2. Enhancement Tasks:
   - Generate comprehensive cross-references between sections
   - Eliminate redundancy while maintaining each section's standalone readability
   - Standardize terminology (prefer PostgreSQL implementation terms: e.g., "buffer" not "cache slot", "page" for the fixed-size block in memory, "block" for on-disk unit, "relation" for table/index)
   - Add navigation aids (Table of Contents, section breadcrumbs, next/prev links)
   - Ensure consistent diagram style and labeling across all Mermaid diagrams

3. Quality Assurance:
   - Verify all key_symbols.txt entries are documented somewhere in the output
   - Ensure logical flow: high-level concepts → architecture → implementation details
   - Validate all internal cross-reference links
   - Check all Mermaid diagrams render correctly (valid syntax)
   - Confirm code examples and source references match actual PostgreSQL source
   - Flag any remaining ambiguities or areas needing community review

4. Output Organization:
   Since total size will likely exceed 2000 lines:
   - Split into logical modules with clear boundaries
   - Create index.md as the navigation hub linking all modules
   - Maintain coherent reading experience with "Prerequisites" and "Next" notes per module
   - Each module should be self-contained enough for targeted reading
   - **All final output files must be written under `topic_specific_generated_docs/about_buffer_management/final/`**
   - **Consolidated diagrams must be copied to `topic_specific_generated_docs/about_buffer_management/diagrams/`**

   Module structure (all under `topic_specific_generated_docs/about_buffer_management/final/`):
   - index.md                               (navigation hub, reading guide)
   - 01_executive_summary.md                (overview for newcomers)
   - 02_architecture_overview.md            (system-wide perspective, main diagram)
   - 03_buffer_pool_architecture.md         (shared memory layout, BufferDesc, initialization)
   - 04_buffer_lookup_and_hashtable.md      (buffer tags, hash table, partitioned locking)
   - 05_buffer_access_protocol.md           (ReadBuffer, pin/unpin, content locks)
   - 06_page_concurrency_control.md         (lock types, lock ordering, per-operation traces, contention)
   - 07_buffer_replacement_policy.md        (clock sweep, usage count, free list, ring buffers)
   - 08_page_layout_and_types.md            (PageHeaderData, line pointers, forks, page types)
   - 09_dirty_buffer_and_writeback.md       (MarkBufferDirty, bgwriter, checkpoint)
   - 10_wal_integration.md                  (WAL-before-data, LSN, full-page writes)
   - 11_storage_manager.md                  (smgr, md.c, VFD, relation forks)
   - 12_data_movement_and_durability.md     (shared buffers ↔ OS cache ↔ disk, fsync)
   - 13_local_buffers.md                    (temp tables, per-backend buffer pool)
   - 14_access_method_integration.md        (heap, B-tree, VACUUM, prefetch)
   - 15_deep_dives.md                       (double buffering, ring buffers, FPW, checksums, lock contention, crash recovery)
   - appendix_symbol_index.md              (alphabetical symbol reference)
   - appendix_glossary.md                  (buffer management terminology)
   - appendix_data_structures.md           (key struct definitions)
   - appendix_guc_parameters.md            (configuration parameters affecting buffer management)

5. Additional Deliverables (also under `topic_specific_generated_docs/about_buffer_management/final/`):
   - buffer_mgmt_quick_reference.md   (2-page summary: key concepts, critical functions, common debugging tips)
   - buffer_mgmt_api_reference.md     (function signatures grouped by subsystem, with brief descriptions)
   - quality_report.md                (coverage metrics: % of key_symbols documented, diagram count, known gaps, improvement suggestions)
```

**Expected Output Check**: Verify professional documentation quality, complete symbol coverage (>80%), and coherent navigation structure.

---

## Orchestration Rules

### Execution Flow
1. **Before Stage 1**: Create the output directory tree:
   ```bash
   mkdir -p topic_specific_generated_docs/about_buffer_management/{stage1,stage2/diagrams,final,diagrams}
   ```
2. Execute each stage sequentially — do not proceed until the previous stage completes successfully
3. Capture all output files from each subagent into the appropriate subdirectory under `topic_specific_generated_docs/about_buffer_management/`
4. Validate expected outputs before proceeding to the next stage
5. Report progress after each stage

### Source Tree Primacy
- The local `./src/` directory is the **single source of truth**. If MCP tool results conflict with the local source code, always prefer the local source.
- Subagents should use `./src/` for structural exploration (file layout, neighboring functions, header inclusions) and MCP tools for indexed cross-reference queries.
- All generated documentation must include verifiable source file paths relative to `./src/`.

### Error Handling
- **Subagent failure**: Retry once with modified parameters (e.g., reduce scope), then proceed with partial results and document gaps
- **Missing expected files**: Log warning, attempt recovery using available data, note in quality_report.md
- **Context limit approaching**: Save progress checkpoint, split remaining work into smaller focused chunks, resume from checkpoint
- **MCP server errors**: Implement exponential backoff (1s, 2s, 4s, max 3 retries) before failing gracefully
- **Symbol not found**: Log missing symbol, attempt alternative names (e.g., with/without `Pg` prefix), continue with available data

### Progress Reporting
After each stage, report:
```
[Stage X Complete]
Generated files: <list>
Key metrics: <symbols processed, diagrams created, coverage %>
Issues encountered: <any warnings or partial failures>
Next stage: <description>
```

### Final Validation
Before declaring completion:
1. Verify all critical path symbols are documented (ReadBuffer, BufferAlloc, StrategyGetBuffer, FlushBuffer, BufferSync, smgrread, smgrwrite, LockBuffer, LockBufferForCleanup, StartBufferIO)
2. Count and list all generated diagrams (must be ≥ 9)
3. Check total documentation coverage against key_symbols.txt (target > 80%)
4. Ensure no broken cross-references or unresolved TODO markers remain
5. Confirm file organization follows the specified module structure
6. Validate all Mermaid diagram syntax

### Success Criteria
The task is complete when:
- [ ] All 3 stages executed successfully
- [ ] Comprehensive buffer management documentation generated covering all 12 functional areas
- [ ] Minimum 9 technical diagrams included and rendering correctly
- [ ] quality_report.md shows > 80% symbol coverage
- [ ] Documentation is organized into navigable modules with index.md
- [ ] Both high-level overview (suitable for newcomers) and deep implementation details (suitable for PostgreSQL contributors) are present
- [ ] Quick reference and API reference supplements are generated

---

## Start Execution
Begin with Stage 1 immediately. Do not wait for confirmation between stages — proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreSQL Buffer Management Documentation Generation - Stage 1: Architecture Analysis"