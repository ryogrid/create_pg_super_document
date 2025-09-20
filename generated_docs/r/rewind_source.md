# rewind_source

## Location
[src/bin/pg_rewind/rewind_source.h:23-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/rewind_source.h#L23-L78)

## Overview
The rewind_source struct defines an abstract interface for data sources used by pg_rewind utility, providing a unified API for accessing files and WAL data from both local and remote PostgreSQL servers.

## Definition

```c
typedef struct rewind_source
{
	/*
	 * Traverse all files in the source data directory, and call 'callback' on
	 * each file.
	 */
	void		(*traverse_files) (struct rewind_source *,
								   process_file_callback_t callback);

	/*
	 * Fetch a single file into a malloc'd buffer. The file size is returned
	 * in *filesize. The returned buffer is always zero-terminated, which is
	 * handy for text files.
	 */
	char	   *(*fetch_file) (struct rewind_source *, const char *path,
							   size_t *filesize);

	/*
	 * Request to fetch (part of) a file in the source system, specified by an
	 * offset and length, and write it to the same offset in the corresponding
	 * target file. The source implementation may queue up the request and
	 * execute it later when convenient. Call finish_fetch() to flush the
	 * queue and execute all requests.
	 */
	void		(*queue_fetch_range) (struct rewind_source *, const char *path,
									  off_t offset, size_t len);

	/*
	 * Like queue_fetch_range(), but requests replacing the whole local file
	 * from the source system. 'len' is the expected length of the file,
	 * although when the source is a live server, the file may change
	 * concurrently. The implementation is not obliged to copy more than 'len'
	 * bytes, even if the file is larger. However, to avoid copying a
	 * truncated version of the file, which can cause trouble if e.g. a
	 * configuration file is modified concurrently, the implementation should
	 * try to copy the whole file, even if it's larger than expected.
	 */
	void		(*queue_fetch_file) (struct rewind_source *, const char *path,
									 size_t len);

	/*
	 * Execute all requests queued up with queue_fetch_range().
	 */
	void		(*finish_fetch) (struct rewind_source *);

	/*
	 * Get the current WAL insert position in the source system.
	 */
	XLogRecPtr	(*get_current_wal_insert_lsn) (struct rewind_source *);

	/*
	 * Free this rewind_source object.
	 */
	void		(*destroy) (struct rewind_source *);

} rewind_source;
```
## Detailed Description
The rewind_source struct serves as a polymorphic interface in the pg_rewind utility, enabling access to PostgreSQL server data through different implementation strategies. This design allows pg_rewind to work with both local file system access and remote connections via libpq. The struct contains function pointers that define the complete contract for data retrieval operations needed during the rewind process, including file traversal, content fetching, and WAL position queries.

The interface supports both immediate and deferred execution models through queuing mechanisms, which is particularly important for optimizing network operations when working with remote servers. All implementations must provide complete functionality for file operations, WAL access, and proper resource cleanup.

## Parameters / Member Variables
- : Function pointer to traverse all files in the source data directory, calling a callback on each file
- : Function pointer to fetch a complete file into a malloc'd buffer with size information
- : Function pointer to queue a request for fetching part of a file (offset and length specified)
- : Function pointer to queue a request for replacing a whole local file from source
- : Function pointer to execute all queued fetch requests
- : Function pointer to get the current WAL insert position from the source system
- : Function pointer to free the rewind_source object and associated resources

## Dependencies
- Functions called/Symbols referenced:
  - process_file_callback_t (callback type for file traversal)
  - XLogRecPtr (WAL position type)
- Called from (representative examples):
  - libpq_source (remote server implementation)
  - local_source (local filesystem implementation)
  - [perform_rewind](../p/perform_rewind.md) (main rewind operation)

## Notes and Other Information
This struct implements the Strategy pattern, allowing pg_rewind to work with different data sources transparently. Two main implementations exist: local_source for direct filesystem access and libpq_source for remote PostgreSQL connections. The queuing mechanism in queue_fetch_range and queue_fetch_file is designed to optimize batch operations, particularly important for network efficiency with remote sources. The interface ensures that all file operations are properly abstracted, making the rewind logic independent of whether the source is local or remote.