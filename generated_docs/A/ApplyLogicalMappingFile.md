# ApplyLogicalMappingFile

## Location
[src/backend/replication/logical/reorderbuffer.c:5211-5302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L5211-L5302)

## Overview
Applies logical rewrite mappings from a file to update tuple command ID data during PostgreSQL logical replication, handling tuple relocations that occur during table rewrites.

## Definition
```c
static void ApplyLogicalMappingFile(HTAB *tuplecid_data, Oid relid, const char *fname)
```

## Detailed Description
This function processes logical rewrite mapping files that are created when PostgreSQL performs table rewrites (such as during ALTER TABLE operations) that affect logical replication. When tables are rewritten, tuples get new physical locations, but logical replication needs to maintain consistency by tracking these location changes.

The function operates by:
1. Opening the specified mapping file from the pg_logical/mappings directory
2. Reading LogicalRewriteMappingData entries sequentially from the file
3. For each mapping entry, looking up the old tuple location in the tuplecid_data hash table
4. If found, creating or updating an entry for the new tuple location with the same command ID information
5. Handling cases where mappings already exist with validation to ensure consistency

The mappings contain both old and new relation locators and tuple identifiers, allowing the system to translate between pre-rewrite and post-rewrite tuple references. This is crucial for maintaining logical replication consistency across table structure changes.

## Parameters / Member Variables
- `tuplecid_data`: Hash table containing tuple command ID mappings that will be updated with new location information
- `relid`: Object ID of the relation being processed (currently unused in the function implementation)
- `fname`: Filename of the mapping file to process, located in pg_logical/mappings directory

## Dependencies
- Functions called/Symbols referenced:
  - sprintf
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - ereport/ERROR
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
  - memset
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end
  - read
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [hash_search](../h/hash_search.md)
  - [CloseTransientFile](../C/CloseTransientFile.md)
- Called from (representative examples):
  - [UpdateLogicalMappings](../U/UpdateLogicalMappings.md)

## Notes and Other Information
- This is a static function, accessible only within reorderbuffer.c
- The function assumes the mapping file has been pre-validated for correctness, commitment, and proper LSN ordering
- Uses PostgreSQL's transient file API for safe file operations with proper error handling
- Includes comprehensive error checking for file operations and data integrity
- The function handles partial updates gracefully - if no existing mapping is found for an old location, it simply continues to the next entry
- Uses assertion checks to validate that existing command ID mappings are consistent when merging
- The relid parameter is currently not used but may be reserved for future functionality or validation
- Critical for maintaining logical replication consistency during DDL operations that physically reorganize table data

## Simplified Source

```c
static void ApplyLogicalMappingFile(HTAB *tuplecid_data, Oid relid, const char *fname)
{
    char path[MAXPGPATH];
    int fd;
    LogicalRewriteMappingData map;

    // Open the mapping file
    sprintf(path, "pg_logical/mappings/%s", fname);
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
        ereport(ERROR, (errmsg("could not open file \"%s\"", path)));

    // Process each mapping entry in the file
    while (true)
    {
        ReorderBufferTupleCidKey key;
        ReorderBufferTupleCidEnt *old_ent, *new_ent;
        bool found;

        // Read next mapping entry
        int readBytes = read(fd, &map, sizeof(LogicalRewriteMappingData));

        // Check for end of file or read errors
        if (readBytes == 0) break;  // EOF
        if (readBytes < 0 || readBytes != sizeof(LogicalRewriteMappingData))
            ereport(ERROR, (errmsg("could not read from file \"%s\"", path)));

        // Look up existing entry for old tuple location
        memset(&key, 0, sizeof(key));
        key.rlocator = map.old_locator;
        ItemPointerCopy(&map.old_tid, &key.tid);

        old_ent = hash_search(tuplecid_data, &key, HASH_FIND, NULL);
        if (!old_ent)
            continue;  // No existing mapping, skip

        // Create or update entry for new tuple location
        key.rlocator = map.new_locator;
        ItemPointerCopy(&map.new_tid, &key.tid);

        new_ent = hash_search(tuplecid_data, &key, HASH_ENTER, &found);

        if (!found)
        {
            // Copy command ID information to new location
            new_ent->cmin = old_ent->cmin;
            new_ent->cmax = old_ent->cmax;
            new_ent->combocid = old_ent->combocid;
        }
        // If found, validate consistency (assertions removed for simplicity)
    }

    // Close the file
    if (CloseTransientFile(fd) != 0)
        ereport(ERROR, (errmsg("could not close file \"%s\"", path)));
}
```