# dir_close

## Location
[src/bin/pg_basebackup/walmethods.c:385-513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L385-L513)

## Overview
Closes a WAL file in the directory-based method, handling compression finalization, file operations (rename/unlink), and resource cleanup based on the specified close method.

## Definition
```c
static int
dir_close(Walfile *f, WalCloseMethod method)
```

## Detailed Description
This function handles closing WAL files in the directory-based method, performing different operations based on the compression algorithm and close method specified. The function manages several critical tasks:

1. **Compression finalization**: For LZ4 compression, writes final compressed data and frees compression context. For gzip, calls gzclose().
2. **File operations**: Depending on the close method:
   - CLOSE_NORMAL with temp_suffix: Renames temporary file to permanent name
   - CLOSE_UNLINK: Deletes the file
   - CLOSE_NORMAL without temp_suffix or CLOSE_NO_RENAME: Syncs file if sync mode enabled
3. **Resource cleanup**: Frees all allocated memory and closes file descriptors
4. **Synchronization**: Performs fsync operations when sync mode is enabled

The function handles three close methods:
- **CLOSE_NORMAL**: Normal file closure, with optional rename if temp_suffix exists
- **CLOSE_UNLINK**: Closes and deletes the file
- **CLOSE_NO_RENAME**: Closes file without renaming, syncing if requested

## Parameters / Member Variables
- `f`: Pointer to Walfile structure representing the file to close
- `method`: WalCloseMethod specifying how to close the file (CLOSE_NORMAL, CLOSE_UNLINK, CLOSE_NO_RENAME)

## Dependencies
- Functions called/Symbols referenced:
  - clear_error (error state initialization)
  - gzclose (gzip file closure)
  - LZ4F_compressEnd, LZ4F_freeCompressionContext (LZ4 compression cleanup)
  - write, close (system calls)
  - [dir_get_file_name](dir_get_file_name.md) (filename construction)
  - rename, durable_rename (file renaming)
  - unlink (file deletion)
  - [fsync_fname](../f/fsync_fname.md), fsync_parent_path (synchronization)
  - [pg_free](../p/pg_free.md) (memory deallocation)
- Called from (representative examples):
  - WAL file management in pg_basebackup

## Notes and Other Information
- This is a static function, only accessible within the walmethods.c file
- Returns 0 on success, -1 on error
- For LZ4 compression, writes final compressed chunk before closing
- Handles both durable and non-durable rename operations based on sync settings
- Performs complete resource cleanup including freeing compression buffers and contexts
- The function sets lasterrno in the wwmethod structure on errors
- File synchronization is performed when sync mode is enabled and appropriate for the close method
- Memory allocated for pathname, fullpath, and temp_suffix is properly freed

## Simplified Source

```c
static int
dir_close(Walfile *f, WalCloseMethod method) {
    int r;
    DirectoryMethodFile *df = (DirectoryMethodFile *) f;
    DirectoryMethodData *dir_data = (DirectoryMethodData *) f->wwmethod;
    char tmppath[MAXPGPATH];
    char tmppath2[MAXPGPATH];

    clear_error(f->wwmethod);

    // Close file based on compression type
    if (f->wwmethod->compression_algorithm == PG_COMPRESSION_GZIP) {
        r = gzclose(df->gzfp);
    } else if (f->wwmethod->compression_algorithm == PG_COMPRESSION_LZ4) {
        // Write final LZ4 compression data
        size_t compressed = LZ4F_compressEnd(df->ctx, df->lz4buf, df->lz4bufsize, NULL);
        if (LZ4F_isError(compressed)) {
            f->wwmethod->lasterrstring = LZ4F_getErrorName(compressed);
            return -1;
        }
        // Write final compressed data
        if (write(df->fd, df->lz4buf, compressed) != compressed) {
            f->wwmethod->lasterrno = errno ? errno : ENOSPC;
            return -1;
        }
        r = close(df->fd);
    } else {
        r = close(df->fd);
    }

    if (r == 0) {
        // Handle different close methods
        if (method == CLOSE_NORMAL && df->temp_suffix) {
            // Rename temporary file to permanent name
            char *temp_filename = dir_get_file_name(f->wwmethod, df->base.pathname, df->temp_suffix);
            char *final_filename = dir_get_file_name(f->wwmethod, df->base.pathname, NULL);

            snprintf(tmppath, sizeof(tmppath), "%s/%s", dir_data->basedir, temp_filename);
            snprintf(tmppath2, sizeof(tmppath2), "%s/%s", dir_data->basedir, final_filename);

            if (f->wwmethod->sync) {
                r = durable_rename(tmppath, tmppath2);
            } else {
                r = rename(tmppath, tmppath2);
            }

            pg_free(temp_filename);
            pg_free(final_filename);
        } else if (method == CLOSE_UNLINK) {
            // Delete the file
            char *filename = dir_get_file_name(f->wwmethod, df->base.pathname, df->temp_suffix);
            snprintf(tmppath, sizeof(tmppath), "%s/%s", dir_data->basedir, filename);
            r = unlink(tmppath);
            pg_free(filename);
        } else {
            // Sync file and directory if needed
            if (f->wwmethod->sync) {
                r = fsync_fname(df->fullpath, false);
                if (r == 0) {
                    r = fsync_parent_path(df->fullpath);
                }
            }
        }
    }

    if (r != 0) {
        f->wwmethod->lasterrno = errno;
    }

    // Cleanup resources
    pg_free(df->lz4buf);
    LZ4F_freeCompressionContext(df->ctx);
    pg_free(df->base.pathname);
    pg_free(df->fullpath);
    pg_free(df->temp_suffix);
    pg_free(df);

    return r;
}
```