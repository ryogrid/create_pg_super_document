# IncrementalBackupInfo

## Location
[src/backend/backup/basebackup_incremental.c:76-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L76-L143)

## Overview
IncrementalBackupInfo is a structure that manages the state and metadata required for performing incremental backups in PostgreSQL, storing information from backup manifests and tracking block-level changes.

## Definition

```c
struct IncrementalBackupInfo
{
	/* Memory context for this object and its subsidiary objects. */
	MemoryContext mcxt;

	/* Temporary buffer for storing the manifest while parsing it. */
	StringInfoData buf;

	/* WAL ranges extracted from the backup manifest. */
	List	   *manifest_wal_ranges;

	/*
	 * Files extracted from the backup manifest.
	 *
	 * We don't really need this information, because we use WAL summaries to
	 * figure out what's changed. It would be unsafe to just rely on the list
	 * of files that existed before, because it's possible for a file to be
	 * removed and a new one created with the same name and different
	 * contents. In such cases, the whole file must still be sent. We can tell
	 * from the WAL summaries whether that happened, but not from the file
	 * list.
	 *
	 * Nonetheless, this data is useful for sanity checking. If a file that we
	 * think we shouldn't need to send is not present in the manifest for the
	 * prior backup, something has gone terribly wrong. We retain the file
	 * names and sizes, but not the checksums or last modified times, for
	 * which we have no use.
	 *
	 * One significant downside of storing this data is that it consumes
	 * memory. If that turns out to be a problem, we might have to decide not
	 * to retain this information, or to make it optional.
	 */
	backup_file_hash *manifest_files;

	/*
	 * Block-reference table for the incremental backup.
	 *
	 * It's possible that storing the entire block-reference table in memory
	 * will be a problem for some users. The in-memory format that we're using
	 * here is pretty efficient, converging to little more than 1 bit per
	 * block for relation forks with large numbers of modified blocks. It's
	 * possible, however, that if you try to perform an incremental backup of
	 * a database with a sufficiently large number of relations on a
	 * sufficiently small machine, you could run out of memory here. If that
	 * turns out to be a problem in practice, we'll need to be more clever.
	 */
	BlockRefTable *brtab;

	/*
	 * State object for incremental JSON parsing
	 */
	JsonManifestParseIncrementalState *inc_state;
};
```
## Detailed Description
IncrementalBackupInfo serves as the central data structure for managing incremental backup operations in PostgreSQL. It maintains all the necessary state information to perform efficient incremental backups by tracking which blocks have been modified since the previous backup. The structure is designed to handle the complex task of parsing backup manifests, managing WAL ranges, and maintaining block-level change tracking for optimal backup performance.

The structure leverages WAL summaries to determine what has changed rather than relying solely on file lists, which provides better safety against scenarios where files are removed and recreated with the same name but different contents. The block-reference table uses an efficient in-memory format that converges to approximately 1 bit per block for relation forks with large numbers of modified blocks.

## Parameters / Member Variables
- : Memory context that manages memory allocation for this object and all its subsidiary objects, ensuring proper cleanup
- : Temporary StringInfo buffer used for storing and parsing backup manifest data during processing
- : List containing WAL ranges extracted from the backup manifest, used to determine the scope of changes
- : Hash table of files from the previous backup manifest, used for sanity checking but not primary change detection
- : Block-reference table that tracks which specific blocks need to be included in the incremental backup
- : State object that maintains context during incremental JSON parsing of backup manifests

## Dependencies
- Functions called/Symbols referenced:
  - BlockRefTable
  - JsonManifestParseIncrementalState
  - [manifest_process_version](../m/manifest_process_version.md)
  - [manifest_process_system_identifier](../m/manifest_process_system_identifier.md)
  - [manifest_process_file](../m/manifest_process_file.md)
  - [manifest_process_wal_range](../m/manifest_process_wal_range.md)
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md)
  - pg_checksum_type
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [SendBaseBackup](../S/SendBaseBackup.md)
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)
  - [GetFileBackupMethod](../G/GetFileBackupMethod.md)
  - [AppendIncrementalManifestData](../A/AppendIncrementalManifestData.md)
  - [FinalizeIncrementalManifest](../F/FinalizeIncrementalManifest.md)

## Notes and Other Information
The structure is designed with memory efficiency in mind, but the authors acknowledge that storing the entire block-reference table in memory could potentially be problematic for very large databases on memory-constrained systems. The current implementation is optimized for most common use cases.

The manifest_files member is retained primarily for sanity checking purposes rather than primary change detection logic. While it consumes additional memory, it provides valuable validation that files expected to be unchanged actually existed in the previous backup.

The structure is located in src/backend/backup/basebackup_incremental.c:76-143 and is integral to PostgreSQL's incremental backup functionality introduced for efficient backup operations.