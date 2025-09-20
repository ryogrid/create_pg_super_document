# rfile

## Location
[src/bin/pg_combinebackup/reconstruct.c:37-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L37-L47)

## Overview
The  struct stores metadata and state information needed to reconstruct files from PostgreSQL backup chains, supporting both full and incremental backups in the pg_combinebackup utility.

## Definition

```c
typedef struct rfile
{
	char	   *filename;
	int			fd;
	size_t		header_length;
	unsigned	num_blocks;
	BlockNumber *relative_block_numbers;
	unsigned	truncation_block_length;
	unsigned	num_blocks_read;
	off_t		highest_offset_read;
} rfile;
```
## Detailed Description
The  struct is a core data structure in PostgreSQL's pg_combinebackup utility that manages file reconstruction from backup chains. For any given output file being reconstructed, one  instance is created per backup that needs to be consulted during the reconstruction process.

The struct supports two modes of operation:
- **Full backup files**: Only  and  are initialized, with remaining fields set to 0 or NULL
- **Incremental backup files**: All fields are utilized to track block-level changes and reconstruction state

The struct maintains both static metadata about the file structure and dynamic state about the reconstruction progress.

## Parameters / Member Variables
- `*filename`: Path to the backup file on disk
- `fd`: File descriptor for the opened backup file
- `header_length`: Size of the incremental backup file header (0 for full backups)
- `num_blocks`: Total number of blocks in the incremental backup
- `*relative_block_numbers`: Array of block numbers present in the incremental backup
- `truncation_block_length`: Length of the truncation block for incremental backups
- `num_blocks_read`: Counter tracking how many blocks have been read (starts at 0)
- `highest_offset_read`: Highest file offset that has been read (starts at 0)
## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (PostgreSQL block number type)
- Called from (representative examples):
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 

## Notes and Other Information
- This struct is specific to the pg_combinebackup utility and is not used in the main PostgreSQL server
- The struct efficiently handles both full and incremental backup scenarios with the same data structure
- Progress tracking fields (, ) enable resumable operations and debugging
- The design allows for block-level reconstruction from incremental backups while maintaining simplicity for full backup processing