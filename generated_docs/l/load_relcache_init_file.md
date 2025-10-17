# load_relcache_init_file

## Location
[src/backend/utils/cache/relcache.c:6075-6490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L6075-L6490)

## Overview
Attempts to load relation cache entries from the shared or local cache initialization file, enabling fast startup by avoiding sequential scans of system catalogs.

## Definition

```c
structure */
		if (fread(rel, 1, len, fp) != len)
			goto read_failed;
```
## Detailed Description
This function is part of PostgreSQL's relation cache initialization optimization system. During normal backend startup, instead of building critical relation descriptors through expensive sequential scans of system catalogs, this function attempts to load pre-built relation cache entries from a binary initialization file.

The function handles both shared catalogs (global initialization file) and local database catalogs (database-specific initialization file). If successful, it populates the relation cache with critical relation descriptors including system tables and indexes, significantly speeding up backend startup.

The function performs extensive validation while reading the file, including magic number checks, structure size verification, and ensuring the correct number of nailed (critical) relations and indexes are loaded. If any validation fails, the function returns false, forcing the system to rebuild the cache the hard way.

For index relations, the function reconstructs complex index-specific data structures including operator families, operator input types, support procedures, collations, and options. For table relations, it initializes table access method data.

## Parameters / Member Variables
- : Boolean flag indicating whether to load the shared initialization file (for shared catalogs) or the local initialization file (for database-specific catalogs)

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)  
  - [InitIndexAmRoutine](../I/InitIndexAmRoutine.md)
  - [RelationInitTableAccessMethod](../R/RelationInitTableAccessMethod.md)
  - [RelationInitLockInfo](../R/RelationInitLockInfo.md)
  - [RelationInitPhysicalAddr](../R/RelationInitPhysicalAddr.md)
  - RelationCacheInsert
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)/MemoryContextAllocZero
  - AllocSetContextCreate
- Called from (representative examples):
  - [RelationCacheInitializePhase2](../R/RelationCacheInitializePhase2.md)
  - [RelationCacheInitializePhase3](../R/RelationCacheInitializePhase3.md)

## Notes and Other Information
- The function operates in CacheMemoryContext and assumes this context is already active
- Sets global flags  or  on success
- Uses magic number RELCACHE_INIT_FILEMAGIC for file format validation
- Validates the count of nailed relations/indexes against expected constants (NUM_CRITICAL_SHARED_RELS, etc.)
- [Complex](../C/Complex.md) data like rules, triggers, RLS policies, and partition info are not saved in the init file and must be rebuilt separately
- The init file mechanism significantly improves startup performance for databases with many system catalog entries
- File location: src/backend/utils/cache/relcache.c:6075-6490

## Simplified Source

```c
static bool load_relcache_init_file(bool shared) {
    FILE *fp;
    char initfilename[MAXPGPATH];
    Relation *rels;
    int relno, num_rels, max_rels, nailed_rels, nailed_indexes, magic;

    // Determine init file path (shared vs local)
    if (shared)
        snprintf(initfilename, sizeof(initfilename), "global/%s", RELCACHE_INIT_FILENAME);
    else
        snprintf(initfilename, sizeof(initfilename), "%s/%s", DatabasePath, RELCACHE_INIT_FILENAME);

    // Try to open the init file
    fp = AllocateFile(initfilename, PG_BINARY_R);
    if (fp == NULL)
        return false;

    // Initialize relation array for loading
    max_rels = 100;
    rels = (Relation *) palloc(max_rels * sizeof(Relation));
    num_rels = nailed_rels = nailed_indexes = 0;

    // Check magic number for file format compatibility
    if (fread(&magic, 1, sizeof(magic), fp) != sizeof(magic) ||
        magic != RELCACHE_INIT_FILEMAGIC)
        goto read_failed;

    // Read each relation from file
    for (relno = 0;; relno++) {
        Size len;
        Relation rel;
        Form_pg_class relform;

        // Read relation descriptor length
        if (fread(&len, 1, sizeof(len), fp) != sizeof(len)) {
            if (len == 0) break; // End of file
            goto read_failed;
        }

        // Validate structure size
        if (len != sizeof(RelationData))
            goto read_failed;

        // Expand relation array if needed
        if (num_rels >= max_rels) {
            max_rels *= 2;
            rels = (Relation *) repalloc(rels, max_rels * sizeof(Relation));
        }

        // Allocate and read relation structure
        rel = rels[num_rels++] = (Relation) palloc(len);
        if (fread(rel, 1, len, fp) != len)
            goto read_failed;

        // Read and attach pg_class tuple
        if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
            goto read_failed;
        relform = (Form_pg_class) palloc(len);
        if (fread(relform, 1, len, fp) != len)
            goto read_failed;
        rel->rd_rel = relform;

        // Initialize tuple descriptor
        rel->rd_att = CreateTemplateTupleDesc(relform->relnatts);
        rel->rd_att->tdrefcount = 1;

        // Read attribute data for each column
        for (int i = 0; i < relform->relnatts; i++) {
            Form_pg_attribute attr = TupleDescAttr(rel->rd_att, i);
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len) ||
                len != ATTRIBUTE_FIXED_PART_SIZE ||
                fread(attr, 1, len, fp) != len)
                goto read_failed;
        }

        // Read relation options if present
        if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
            goto read_failed;
        if (len > 0) {
            rel->rd_options = palloc(len);
            if (fread(rel->rd_options, 1, len, fp) != len)
                goto read_failed;
        } else {
            rel->rd_options = NULL;
        }

        // Special processing for indexes
        if (rel->rd_rel->relkind == RELKIND_INDEX) {
            if (rel->rd_isnailed) nailed_indexes++;

            // Read index-specific data (simplified)
            // [Read pg_index tuple, operator families, support procs, etc.]

            InitIndexAmRoutine(rel);
        } else {
            if (rel->rd_isnailed) nailed_rels++;

            // Initialize table access method for tables
            if (RELKIND_HAS_TABLE_AM(rel->rd_rel->relkind))
                RelationInitTableAccessMethod(rel);
        }

        // Initialize physical addressing and lock info
        RelationInitLockInfo(rel);
        RelationInitPhysicalAddr(rel);
    }

    // Validate we got the expected number of critical relations/indexes
    int expected_rels = shared ? NUM_CRITICAL_SHARED_RELS : NUM_CRITICAL_LOCAL_RELS;
    int expected_indexes = shared ? NUM_CRITICAL_SHARED_INDEXES : NUM_CRITICAL_LOCAL_INDEXES;

    if (nailed_rels != expected_rels || nailed_indexes != expected_indexes) {
        elog(WARNING, "found %d nailed rels and %d nailed indexes, expected %d and %d",
             nailed_rels, nailed_indexes, expected_rels, expected_indexes);
        goto read_failed;
    }

    // Insert all relations into the cache
    for (relno = 0; relno < num_rels; relno++)
        RelationCacheInsert(rels[relno], false);

    pfree(rels);
    FreeFile(fp);

    // Mark critical caches as built
    if (shared)
        criticalSharedRelcachesBuilt = true;
    else
        criticalRelcachesBuilt = true;
    return true;

read_failed:
    pfree(rels);
    FreeFile(fp);
    return false;
}
```