# tar_open_for_write

## Location
[src/bin/pg_basebackup/walmethods.c:837-1006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L837-L1006)

## Overview
Opens a new file for writing within a TAR archive, handling TAR header creation, compression setup, and initial file positioning for PostgreSQL WAL operations.

## Definition
```c
static Walfile *tar_open_for_write(WalWriteMethod *wwmethod, const char *pathname, const char *temp_suffix, size_t pad_to_size)
```

## Detailed Description
This function is responsible for opening a new file within a TAR archive for writing. It performs several critical operations: opens the TAR file if not already open, initializes compression if gzip is enabled, ensures only one file is open at a time, creates TAR headers with proper metadata, handles compression parameter adjustments for headers vs content, and manages file padding for uncompressed files. The function integrates closely with PostgreSQL's WAL archiving system and supports both compressed and uncompressed TAR archives.

## Parameters / Member Variables
- `wwmethod`: Pointer to WalWriteMethod structure containing configuration and state
- `pathname`: Base pathname for the file to be created within the TAR archive
- `temp_suffix`: Optional temporary suffix for the filename (may be NULL)
- `pad_to_size`: Size to pad the file to (0 for no padding)

## Dependencies
- Functions called/Symbols referenced:
  - clear_error (error state reset)
  - open (system call for file opening)
  - [tar_get_file_name](tar_get_file_name.md) (filename construction)
  - tarCreateHeader (TAR header creation)
  - [tar_write_compressed_data](tar_write_compressed_data.md) (compression handling)
  - [tar_write_padding_data](tar_write_padding_data.md) (padding data writer)
  - deflateInit2, deflateParams (zlib compression functions)
  - lseek, write (file I/O system calls)
  - [pg_malloc0](../p/pg_malloc0.md), pg_malloc, pg_free, pg_strdup (PostgreSQL memory functions)
  - Various constants: PG_COMPRESSION_GZIP, PG_COMPRESSION_NONE, TAR_BLOCK_SIZE, etc.
- Called from:
  - [CreateWalDirectoryMethod](../C/CreateWalDirectoryMethod.md) (as function pointer assignment)

## Notes and Other Information
- Returns pointer to Walfile structure on success, NULL on failure
- Enforces single-file-at-a-time constraint for TAR archives
- Lazy-opens the TAR file only when first needed
- For gzip compression, temporarily disables compression for TAR headers
- Handles file padding differently for compressed vs uncompressed files
- Sets file permissions to S_IRUSR | S_IWUSR (user read/write only)
- Maintains current file position tracking and start offset information
- Critical component of PostgreSQL's WAL archiving infrastructure supporting both streaming and file-based backup operations

## Simplified Source

```c
static Walfile *
tar_open_for_write(WalWriteMethod *wwmethod, const char *pathname,
                   const char *temp_suffix, size_t pad_to_size) {
    TarMethodData *tar_data = (TarMethodData *) wwmethod;
    char *tmppath;

    clear_error(wwmethod);

    // Open TAR file if not already open
    if (tar_data->fd < 0) {
        tar_data->fd = open(tar_data->tarfilename,
                           O_WRONLY | O_CREAT | PG_BINARY,
                           pg_file_create_mode);
        if (tar_data->fd < 0) {
            wwmethod->lasterrno = errno;
            return NULL;
        }

        // Initialize gzip compression if enabled
        if (wwmethod->compression_algorithm == PG_COMPRESSION_GZIP) {
            // Setup zlib compression context
            tar_data->zp = (z_streamp) pg_malloc(sizeof(z_stream));
            // Initialize compression parameters
            // (detailed compression setup omitted for brevity)
        }
    }

    // Ensure only one file open at a time
    if (tar_data->currentfile != NULL) {
        wwmethod->lasterrstring =
            _("implementation error: tar files can't have more than one open file");
        return NULL;
    }

    // Create new file structure
    tar_data->currentfile = pg_malloc0(sizeof(TarMethodFile));
    tar_data->currentfile->base.wwmethod = wwmethod;

    // Generate filename and create TAR header
    tmppath = tar_get_file_name(wwmethod, pathname, temp_suffix);
    if (tarCreateHeader(tar_data->currentfile->header, tmppath, NULL, 0,
                       S_IRUSR | S_IWUSR, 0, 0, time(NULL)) != TAR_OK) {
        pg_free(tar_data->currentfile);
        pg_free(tmppath);
        tar_data->currentfile = NULL;
        wwmethod->lasterrstring = _("could not create tar header");
        return NULL;
    }
    pg_free(tmppath);

    // Handle compression parameter adjustments for header
    if (wwmethod->compression_algorithm == PG_COMPRESSION_GZIP) {
        // Flush existing data and disable compression for header
        tar_write_compressed_data(tar_data, NULL, 0, true);
        deflateParams(tar_data->zp, 0, Z_DEFAULT_STRATEGY);
    }

    // Record start position and write header
    tar_data->currentfile->ofs_start = lseek(tar_data->fd, 0, SEEK_CUR);
    tar_data->currentfile->base.currpos = 0;

    if (wwmethod->compression_algorithm == PG_COMPRESSION_NONE) {
        // Write header directly for uncompressed files
        if (write(tar_data->fd, tar_data->currentfile->header, TAR_BLOCK_SIZE) != TAR_BLOCK_SIZE) {
            wwmethod->lasterrno = errno ? errno : ENOSPC;
            pg_free(tar_data->currentfile);
            tar_data->currentfile = NULL;
            return NULL;
        }
    } else if (wwmethod->compression_algorithm == PG_COMPRESSION_GZIP) {
        // Write header through compression without actual compression
        tar_write_compressed_data(tar_data, tar_data->currentfile->header, TAR_BLOCK_SIZE, true);
        // Re-enable compression for file content
        deflateParams(tar_data->zp, wwmethod->compression_level, Z_DEFAULT_STRATEGY);
    }

    tar_data->currentfile->base.pathname = pg_strdup(pathname);

    // Handle file padding if requested
    if (pad_to_size) {
        tar_data->currentfile->pad_to_size = pad_to_size;
        if (wwmethod->compression_algorithm == PG_COMPRESSION_NONE) {
            // Pre-pad uncompressed files
            if (!tar_write_padding_data(tar_data->currentfile, pad_to_size)) {
                return NULL;
            }
            // Seek back to start of data area
            lseek(tar_data->fd, tar_data->currentfile->ofs_start + TAR_BLOCK_SIZE, SEEK_SET);
            tar_data->currentfile->base.currpos = 0;
        }
    }

    return &tar_data->currentfile->base;
}
```