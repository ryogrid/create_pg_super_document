# dir_open_for_write

## Location
[src/bin/pg_basebackup/walmethods.c:117-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L117-L303)

## Overview
Opens a WAL file for writing in directory-based WAL method, supporting multiple compression algorithms and proper file initialization with padding and synchronization.

## Definition
```c
static Walfile *
dir_open_for_write(WalWriteMethod *wwmethod, const char *pathname,
                   const char *temp_suffix, size_t pad_to_size)
```

## Detailed Description
This function is the core file opening method for directory-based WAL writing in pg_basebackup. It creates and initializes a WAL file for writing, handling multiple compression algorithms (none, gzip, LZ4) and performing necessary setup operations. The function handles file creation, compression initialization, pre-padding for uncompressed files, and optional synchronization to ensure data durability.

Key operations performed:
1. Constructs the full file path using dir_get_file_name
2. Opens the file with appropriate flags (O_WRONLY | O_CREAT)
3. Initializes compression context if compression is enabled
4. Performs pre-padding for uncompressed files if requested
5. Optionally syncs the file and directory for durability
6. Creates and initializes the DirectoryMethodFile structure

The function supports both synchronous and asynchronous modes, with synchronous mode ensuring immediate durability through fsync operations.

## Parameters / Member Variables
- `wwmethod`: Pointer to WalWriteMethod containing compression and sync settings
- `pathname`: Base pathname for the WAL file to be created
- `temp_suffix`: Optional temporary suffix for the filename (can be NULL)
- `pad_to_size`: Size to pre-pad the file (only applies to uncompressed files)

## Dependencies
- Functions called/Symbols referenced:
  - [dir_get_file_name](dir_get_file_name.md) (filename construction)
  - clear_error (error state initialization)
  - open (file creation)
  - [pg_pwrite_zeros](../p/pg_pwrite_zeros.md) (file pre-padding)
  - [fsync_fname](../f/fsync_fname.md), fsync_parent_path (synchronization)
  - [pg_malloc0](../p/pg_malloc0.md), pg_free, pg_strdup (memory management)
  - Compression library functions (gzdopen, LZ4F_createCompressionContext, etc.)
- Called from (representative examples):
  - WAL writing methods in pg_basebackup

## Notes and Other Information
- This is a static function, only accessible within the walmethods.c file
- Supports three compression algorithms: none, gzip, and LZ4
- For compressed files, initializes compression contexts and writes headers
- Pre-padding is only performed on uncompressed files to optimize I/O
- In synchronous mode, performs fsync on both file and containing directory
- Returns NULL on error, with error details stored in wwmethod->lasterrno or lasterrstring
- The returned Walfile structure must be properly closed using dir_close
- File descriptor is tracked for all files (compressed and uncompressed) for sync operations

## Simplified Source

```c
static Walfile *
dir_open_for_write(WalWriteMethod *wwmethod, const char *pathname,
                   const char *temp_suffix, size_t pad_to_size) {
    DirectoryMethodData *dir_data = (DirectoryMethodData *) wwmethod;
    char tmppath[MAXPGPATH];
    char *filename;
    int fd;
    DirectoryMethodFile *f;

    clear_error(wwmethod);

    // Construct full file path
    filename = dir_get_file_name(wwmethod, pathname, temp_suffix);
    snprintf(tmppath, sizeof(tmppath), "%s/%s", dir_data->basedir, filename);
    pg_free(filename);

    // Create the file
    fd = open(tmppath, O_WRONLY | O_CREAT | PG_BINARY, pg_file_create_mode);
    if (fd < 0) {
        wwmethod->lasterrno = errno;
        return NULL;
    }

    // Initialize compression if needed
    if (wwmethod->compression_algorithm == PG_COMPRESSION_GZIP) {
        // Setup gzip compression context
        // (compression setup details omitted for brevity)
    }
    if (wwmethod->compression_algorithm == PG_COMPRESSION_LZ4) {
        // Setup LZ4 compression context and write header
        // (compression setup details omitted for brevity)
    }

    // Pre-pad uncompressed files if requested
    if (pad_to_size && wwmethod->compression_algorithm == PG_COMPRESSION_NONE) {
        if (pg_pwrite_zeros(fd, pad_to_size, 0) < 0) {
            wwmethod->lasterrno = errno;
            close(fd);
            return NULL;
        }
        // Reset file position after padding
        lseek(fd, 0, SEEK_SET);
    }

    // Sync file and directory if in sync mode
    if (wwmethod->sync) {
        if (fsync_fname(tmppath, false) != 0 || fsync_parent_path(tmppath) != 0) {
            wwmethod->lasterrno = errno;
            close(fd);
            return NULL;
        }
    }

    // Create and initialize file structure
    f = pg_malloc0(sizeof(DirectoryMethodFile));
    f->base.wwmethod = wwmethod;
    f->base.currpos = 0;
    f->base.pathname = pg_strdup(pathname);
    f->fd = fd;
    f->fullpath = pg_strdup(tmppath);
    if (temp_suffix)
        f->temp_suffix = pg_strdup(temp_suffix);

    return &f->base;
}
```