# f_smgr

## Location
[src/backend/storage/smgr/smgr.c:74-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L74-L105)

## Overview
The f_smgr struct defines the API between smgr.c and individual storage manager modules, providing function pointers for all storage management operations in PostgreSQL.

## Definition

```c
typedef struct f_smgr
{
	void		(*smgr_init) (void);	/* may be NULL */
	void		(*smgr_shutdown) (void);	/* may be NULL */
	void		(*smgr_open) (SMgrRelation reln);
	void		(*smgr_close) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_create) (SMgrRelation reln, ForkNumber forknum,
								bool isRedo);
	bool		(*smgr_exists) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_unlink) (RelFileLocatorBackend rlocator, ForkNumber forknum,
								bool isRedo);
	void		(*smgr_extend) (SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, const void *buffer, bool skipFsync);
	void		(*smgr_zeroextend) (SMgrRelation reln, ForkNumber forknum,
									BlockNumber blocknum, int nblocks, bool skipFsync);
	bool		(*smgr_prefetch) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum, int nblocks);
	void		(*smgr_readv) (SMgrRelation reln, ForkNumber forknum,
							   BlockNumber blocknum,
							   void **buffers, BlockNumber nblocks);
	void		(*smgr_writev) (SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum,
								const void **buffers, BlockNumber nblocks,
								bool skipFsync);
	void		(*smgr_writeback) (SMgrRelation reln, ForkNumber forknum,
								   BlockNumber blocknum, BlockNumber nblocks);
	BlockNumber (*smgr_nblocks) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_truncate) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber old_blocks, BlockNumber nblocks);
	void		(*smgr_immedsync) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_registersync) (SMgrRelation reln, ForkNumber forknum);
} f_smgr;
```
## Detailed Description
The f_smgr struct serves as an abstraction layer that defines the complete interface for storage manager implementations in PostgreSQL. It uses function pointers to provide a pluggable architecture where different storage backends can be implemented by providing implementations for these function pointers. This design allows PostgreSQL to support different storage mechanisms while maintaining a consistent internal API.

The struct encompasses all fundamental storage operations including initialization, file management, I/O operations, synchronization, and cleanup. Storage manager subfunctions are generally expected to report problems via elog(ERROR), with the notable exception of smgr_unlink which should use elog(WARNING) since it's typically called during post-commit/abort cleanup where raising an error would be too late.

## Parameters / Member Variables
- `(void)`: Optional initialization function for the storage manager (may be NULL)
- `(void)`: Optional shutdown function for cleanup (may be NULL)
- `reln)`: Opens a storage manager relation for access
- `forknum)`: Closes a specific fork of a storage manager relation
- `isRedo)`: Creates a new fork of a relation, with redo flag for recovery
- `forknum)`: Checks if a specific fork of a relation exists
- `isRedo)`: Removes a relation fork from storage (uses WARNING instead of ERROR)
- `skipFsync)`: Extends a relation by adding a new block with data
- `skipFsync)`: Extends a relation by adding multiple zero-filled blocks
- `nblocks)`: Prefetches blocks into memory for improved performance
- `nblocks)`: Reads multiple blocks from a relation fork into buffers
- `skipFsync)`: Writes multiple blocks from buffers to a relation fork
- `nblocks)`: Issues writeback hints for blocks to optimize I/O
- `forknum)`: Returns the number of blocks in a relation fork
- `nblocks)`: Truncates a relation fork to a specified number of blocks
- `forknum)`: Immediately synchronizes a relation fork to storage
- `forknum)`: Registers a relation fork for later synchronization
## Dependencies
- Functions called/Symbols referenced:
  - SMgrRelation
  - [RelFileLocatorBackend](../R/RelFileLocatorBackend.md)
  - [ForkNumber](../F/ForkNumber.md)
  - BlockNumber
- Called from (representative examples):
  - Storage manager implementations (like md.c for magnetic disk storage)
  - smgr.c functions that dispatch to storage manager backends

## Notes and Other Information
This struct is central to PostgreSQL's storage management architecture, providing the contract that all storage manager implementations must fulfill. The design allows for flexibility in storage backends while maintaining a consistent interface. Special consideration is given to error handling during different phases of operation (normal operation vs. bootstrap/WAL recovery), and the unlink operation is specifically designed to be more forgiving during cleanup phases to avoid cascading errors during transaction abort or commit cleanup.