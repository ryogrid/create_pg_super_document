# write_relcache_init_file

## Location
[src/backend/utils/cache/relcache.c:6491-6702](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L6491-L6702)

## Overview
Writes out a new initialization file containing the current contents of the relation cache, enabling fast startup for subsequent backend processes.

## Definition

```c
static void
write_relcache_init_file(bool shared)
```
## Detailed Description
This function creates a binary initialization file containing pre-built relation cache entries to optimize backend startup performance. It writes either shared catalog relations or local database relations based on the shared parameter.

The function implements a safe write strategy using temporary files to prevent corruption if another backend attempts to read during the write process. It first writes to a temporary file with the process ID appended, then atomically renames it to the final filename.

The function validates that no relcache invalidation messages have been received during the write process, ensuring data consistency. If invalidations are detected, the temporary file is deleted rather than installed, leaving initialization to a future backend.

For each qualifying relation, the function writes the complete relation descriptor including tuple descriptors, attribute information, access method options, and for indexes, additional metadata like operator families, support procedures, collations, and index options.

The function uses write_item() as a helper to write individual data structures with their sizes to the binary file format.

## Parameters / Member Variables
- `shared`: Boolean flag indicating whether to write shared catalog relations (true) or local database relations (false)
## Dependencies
- Functions called/Symbols referenced:
  - [write_item](write_item.md)
  - [AllocateFile](../A/AllocateFile.md)/FreeFile
  - [RelationIdIsInInitFile](../R/RelationIdIsInInitFile.md)
  - [hash_seq_init](../h/hash_seq_init.md)/hash_seq_search
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
- Called from (representative examples):
  - [RelationCacheInitializePhase3](../R/RelationCacheInitializePhase3.md) (inferred from context)
  - Relcache invalidation handlers

## Notes and Other Information
- Uses magic number RELCACHE_INIT_FILEMAGIC for file format identification
- Implements atomic file replacement using temporary files and rename()
- Checks for relcache invalidation messages throughout the process via relcacheInvalsReceived
- Uses RelCacheInitLock for serialization during final validation and file installation
- File naming convention includes process ID for temporary files to avoid conflicts
- Filters relations based on RelationIdIsInInitFile() for local databases
- [Complex](../C/Complex.md) index metadata is fully preserved including operator classes and support functions
- File location: src/backend/utils/cache/relcache.c:6491-6702

## Simplified Source

```c
static void
write_relcache_init_file(bool shared)
{
    FILE *fp;
    char tempfilename[MAXPGPATH];
    char finalfilename[MAXPGPATH];
    int magic;
    HASH_SEQ_STATUS status;
    RelIdCacheEnt *idhentry;

    // Skip if we've already received relcache invalidations
    if (relcacheInvalsReceived != 0L)
        return;

    // Build temporary and final filenames
    if (shared) {
        snprintf(tempfilename, sizeof(tempfilename), "global/%s.%d",
                RELCACHE_INIT_FILENAME, MyProcPid);
        snprintf(finalfilename, sizeof(finalfilename), "global/%s",
                RELCACHE_INIT_FILENAME);
    } else {
        snprintf(tempfilename, sizeof(tempfilename), "%s/%s.%d",
                DatabasePath, RELCACHE_INIT_FILENAME, MyProcPid);
        snprintf(finalfilename, sizeof(finalfilename), "%s/%s",
                DatabasePath, RELCACHE_INIT_FILENAME);
    }

    // Create temporary file
    unlink(tempfilename);
    fp = AllocateFile(tempfilename, PG_BINARY_W);
    if (fp == NULL) {
        ereport(WARNING, (errmsg("could not create init file \"%s\"", tempfilename)));
        return;
    }

    // Write file magic number for version identification
    magic = RELCACHE_INIT_FILEMAGIC;
    if (fwrite(&magic, 1, sizeof(magic), fp) != sizeof(magic))
        ereport(FATAL, (errmsg_internal("could not write init file")));

    // Write all appropriate relations from the cache
    hash_seq_init(&status, RelationIdCache);
    while ((idhentry = (RelIdCacheEnt *) hash_seq_search(&status)) != NULL) {
        Relation rel = idhentry->reldesc;
        Form_pg_class relform = rel->rd_rel;

        // Filter by shared/local and inclusion criteria
        if (relform->relisshared != shared)
            continue;
        if (!shared && !RelationIdIsInInitFile(RelationGetRelid(rel)))
            continue;

        // Write relation data
        write_item(rel, sizeof(RelationData), fp);
        write_item(relform, CLASS_TUPLE_SIZE, fp);

        // Write attribute descriptors
        for (int i = 0; i < relform->relnatts; i++) {
            write_item(TupleDescAttr(rel->rd_att, i),
                      ATTRIBUTE_FIXED_PART_SIZE, fp);
        }

        // Write access method options
        write_item(rel->rd_options,
                  (rel->rd_options ? VARSIZE(rel->rd_options) : 0), fp);

        // Write index-specific data if this is an index
        if (rel->rd_rel->relkind == RELKIND_INDEX) {
            write_item(rel->rd_indextuple,
                      HEAPTUPLESIZE + rel->rd_indextuple->t_len, fp);
            write_item(rel->rd_opfamily, relform->relnatts * sizeof(Oid), fp);
            write_item(rel->rd_opcintype, relform->relnatts * sizeof(Oid), fp);
            write_item(rel->rd_support,
                      relform->relnatts * (rel->rd_indam->amsupport * sizeof(RegProcedure)), fp);
            write_item(rel->rd_indcollation, relform->relnatts * sizeof(Oid), fp);
            write_item(rel->rd_indoption, relform->relnatts * sizeof(int16), fp);

            // Write per-column operator options
            for (int i = 0; i < relform->relnatts; i++) {
                bytea *opt = rel->rd_opcoptions[i];
                write_item(opt, opt ? VARSIZE(opt) : 0, fp);
            }
        }
    }

    if (FreeFile(fp))
        ereport(FATAL, (errmsg_internal("could not write init file")));

    // Atomic installation with invalidation check
    LWLockAcquire(RelCacheInitLock, LW_EXCLUSIVE);
    AcceptInvalidationMessages();

    if (relcacheInvalsReceived == 0L) {
        // No invalidations received - safe to install the file
        if (rename(tempfilename, finalfilename) < 0)
            unlink(tempfilename);
    } else {
        // Invalidations received - discard the obsolete file
        unlink(tempfilename);
    }

    LWLockRelease(RelCacheInitLock);
}
```