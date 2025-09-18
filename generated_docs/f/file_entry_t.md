# file_entry_t

## Location
[src/bin/pg_rewind/filemap.h:49-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.h#L49-L82)

## Overview
The file_entry_t structure represents information about files found in both local and remote PostgreSQL systems during pg_rewind operations, including their status, properties, and planned actions.

## Definition


## Detailed Description
The file_entry_t structure is a comprehensive representation of file information used by PostgreSQL's pg_rewind utility. It stores detailed information about files present in both the source and target PostgreSQL clusters, enabling the rewind process to make informed decisions about what actions to take for each file. The structure maintains separate status information for both target and source systems, tracks which pages need to be overwritten for relation files, and stores the final action to be performed on each file.

## Parameters / Member Variables
- : Hash table status field used when the entry is stored in a hash table
- : File path relative to the PostgreSQL data directory
- : Boolean flag indicating whether this is a relation data file (table/index data)
- : Whether the file exists in the target cluster
- : Type of the file in the target cluster (regular file, directory, symlink, etc.)
- : Size of the file in the target cluster (for regular files)
- : Target path for symbolic links in the target cluster
- : Bitmap of pages that were modified in the target and need replacement
- : Whether the file exists in the source cluster
- : Type of the file in the source cluster
- : Size of the file in the source cluster (for regular files)
- : Target path for symbolic links in the source cluster
- : The determined action to perform on this file during the rewind process

## Dependencies
- Functions called/Symbols referenced:
  - file_type_t (enum for file types)
  - [datapagemap_t](../d/datapagemap_t.md) (structure for tracking modified pages)
  - file_action_t (enum for file actions)
- Called from (representative examples):
  - [insert_filehash_entry](../i/insert_filehash_entry.md) (creates and initializes file entries)
  - [process_source_file](../p/process_source_file.md) (populates source information)
  - [process_target_file](../p/process_target_file.md) (populates target information)
  - [decide_file_action](../d/decide_file_action.md) (determines the action to take)
  - [perform_rewind](../p/perform_rewind.md) (executes actions on file entries)

## Notes and Other Information
This structure is central to pg_rewind's operation, serving as the primary data structure for tracking file differences between PostgreSQL clusters. It's initially stored in hash tables during information gathering, then sorted into arrays for action execution. The structure efficiently handles different file types including regular files, directories, and symbolic links, with special handling for relation files that may require page-level synchronization.