# BufFile

## Location
[src/backend/storage/file/buffile.c:70-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L70-L117)

## Overview
BufFile is a data structure that represents a buffered file consisting of one or more physical files, each accessed through virtual file descriptors managed by fd.c. It provides a high-level abstraction for handling large files that may be split across multiple physical segments.

## Definition

```c
struct BufFile
{
	int			numFiles;		/* number of physical files in set */
	/* all files except the last have length exactly MAX_PHYSICAL_FILESIZE */
	File	   *files;			/* palloc'd array with numFiles entries */

	bool		isInterXact;	/* keep open over transactions? */
	bool		dirty;			/* does buffer need to be written? */
	bool		readOnly;		/* has the file been set to read only? */

	FileSet    *fileset;		/* space for fileset based segment files */
	const char *name;			/* name of fileset based BufFile */

	/*
	 * resowner is the ResourceOwner to use for underlying temp files.  (We
	 * don't need to remember the memory context we're using explicitly,
	 * because after creation we only repalloc our arrays larger.)
	 */
	ResourceOwner resowner;

	/*
	 * "current pos" is position of start of buffer within the logical file.
	 * Position as seen by user of BufFile is (curFile, curOffset + pos).
	 */
	int			curFile;		/* file index (0..n) part of current pos */
	off_t		curOffset;		/* offset part of current pos */
	int			pos;			/* next read/write position in buffer */
	int			nbytes;			/* total # of valid bytes in buffer */

	/*
	 * XXX Should ideally us PGIOAlignedBlock, but might need a way to avoid
	 * wasting per-file alignment padding when some users create many files.
	 */
	PGAlignedBlock buffer;
};
```
## Detailed Description
BufFile provides a buffered I/O abstraction for handling large files in PostgreSQL. The key design principle is to split large files into multiple physical segments, each limited to MAX_PHYSICAL_FILESIZE bytes, while presenting a unified logical view to the application.

The structure maintains an internal buffer (PGAlignedBlock) to optimize I/O operations by reducing the number of system calls. Read and write operations are buffered, and the buffer is flushed to disk when necessary. The buffering system tracks the current position within the logical file using a combination of file index (curFile), offset within that file (curOffset), and position within the buffer (pos).

BufFile supports two main modes of operation:
1. **Temporary files**: Created using BufFileCreateTemp, managed by the temp file subsystem
2. **FileSet-based files**: Created using BufFileCreateFileSet, allowing shared access across processes

The structure also supports transaction-spanning files (isInterXact flag) and read-only mode for optimization purposes.

## Parameters / Member Variables
- : Number of physical files that make up this logical buffered file
- : Dynamically allocated array of File handles, one for each physical segment
- : Flag indicating whether the file should remain open across transaction boundaries
- : Flag indicating whether the current buffer contains modified data that needs to be written to disk
- : Flag indicating whether the file has been set to read-only mode for optimization
- : Pointer to FileSet structure for shared, named temporary files
- : Name identifier for FileSet-based BufFiles
- : ResourceOwner responsible for cleanup of underlying temporary files
- : Index (0-based) of the currently active physical file segment
- : Byte offset within the current physical file where the buffer starts
- : Current read/write position within the internal buffer
- : Number of valid bytes currently stored in the buffer
- : Internal I/O buffer using aligned block allocation for performance

## Dependencies
- Functions called/Symbols referenced:
  - File (virtual file descriptor type from fd.c)
  - [FileSet](../F/FileSet.md) (for shared temporary file management)
  - [ResourceOwner](../R/ResourceOwner.md) (for resource cleanup)
  - PGAlignedBlock (for aligned memory allocation)
  - [makeBufFileCommon](../m/makeBufFileCommon.md) (internal constructor helper)
  - [makeBufFile](../m/makeBufFile.md) (creates BufFile from existing File)
  - [extendBufFile](../e/extendBufFile.md) (adds new physical segment)
  - [BufFileLoadBuffer](BufFileLoadBuffer.md) (loads data from disk to buffer)
  - [BufFileDumpBuffer](BufFileDumpBuffer.md) (writes buffer contents to disk)
  - [BufFileFlush](BufFileFlush.md) (ensures all data is written to disk)
  - [MakeNewFileSetSegment](../M/MakeNewFileSetSegment.md) (creates new segment in FileSet)

- Called from (representative examples):
  - [ExecHashTableCreate](../E/ExecHashTableCreate.md) (hash join temporary files)
  - [ExecHashIncreaseNumBatches](../E/ExecHashIncreaseNumBatches.md) (hash join batch management)
  - [LogicalTapeSet](../L/LogicalTapeSet.md) (sorting operations)
  - TuplestoreState (tuple storage)
  - [GISTBuildBuffers](../G/GISTBuildBuffers.md) (GiST index construction)
  - [SharedTuplestoreAccessor](../S/SharedTuplestoreAccessor.md) (shared tuple storage)
  - Various replication worker functions

## Notes and Other Information
- [BufFile](BufFile.md) is primarily used for temporary file operations in PostgreSQL, especially for large datasets that don't fit in memory
- The multi-file design allows the system to handle files larger than what the underlying filesystem might support for individual files
- The buffering mechanism significantly improves performance by reducing system call overhead
- [FileSet](../F/FileSet.md)-based BufFiles enable sharing of temporary data between parallel processes
- The structure is designed to work efficiently with PostgreSQL's resource management system
- Buffer alignment (PGAlignedBlock) helps optimize I/O performance on systems that benefit from aligned memory access
- The readOnly flag allows for optimizations when the file will only be read sequentially
- Transaction boundaries are respected through the isInterXact flag, ensuring proper cleanup behavior