# tar_close

## Location
[src/bin/pg_basebackup/walmethods.c:1042-1218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L1042-L1218)

## Overview
Closes a WAL file within a TAR archive, performing final operations including padding, header updates, and synchronization to persistent storage.

## Definition
```c
static int tar_close(Walfile *f, WalCloseMethod method)
```

## Detailed Description
This function implements the close operation for TAR-based WAL writing method in pg_basebackup. It's a complex function that handles multiple scenarios and performs several critical operations:

1. **Unlink Support**: When method is CLOSE_UNLINK, it truncates the TAR file to remove the just-written file entry (only supported for uncompressed files)

2. **Padding Management**: Handles padding to specified file sizes, with different approaches for compressed vs uncompressed files

3. **TAR Format Compliance**: Adds necessary padding to make file size a multiple of TAR_BLOCK_SIZE

4. **Header Updates**: Updates the TAR header with the final file size and recalculates the checksum

5. **Compression Handling**: Special logic for compressed TAR files, including flushing compressed data and temporarily disabling compression for header updates

6. **Synchronization**: Always performs fsync to ensure data is written to persistent storage

The function ensures TAR format compliance while supporting both compressed and uncompressed files.

## Parameters / Member Variables
- `f`: Pointer to the Walfile structure representing the open WAL file within the TAR method
- `method`: Enumeration value indicating how to close the file (CLOSE_NORMAL or CLOSE_UNLINK)

## Dependencies
- Functions called/Symbols referenced:
  - clear_error
  - ftruncate
  - [pg_free](../p/pg_free.md)
  - [tar_write_padding_data](tar_write_padding_data.md)
  - [tarPaddingBytesRequired](tarPaddingBytesRequired.md)
  - [tar_write](tar_write.md)
  - [tar_write_compressed_data](tar_write_compressed_data.md)
  - [print_tar_number](../p/print_tar_number.md)
  - [strlcpy](../s/strlcpy.md)
  - [tarChecksum](tarChecksum.md)
  - lseek
  - write
  - deflateParams (zlib)
  - [tar_sync](tar_sync.md)
  - [GetLastWalMethodError](../G/GetLastWalMethodError.md)
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [CreateWalDirectoryMethod](../C/CreateWalDirectoryMethod.md) (function pointer assignment)
  - [tar_finish](tar_finish.md)

## Notes and Other Information
- This function is marked as static, meaning it's only accessible within the walmethods.c file
- Returns 0 on success, -1 on error with appropriate error information set
- CLOSE_UNLINK is only supported for uncompressed files
- The function performs complex header manipulation to update file size and checksum in the TAR archive
- For compressed files, compression parameters are temporarily modified to write the updated header
- Always calls tar_sync() to ensure data persistence, with a pg_fatal() call if sync fails
- Handles both padding requested at file creation time and TAR format-required padding
- Memory cleanup is performed for both the file pathname and the TarMethodFile structure
- The function seeks back to the end of the file after header updates to prepare for the next file

## Simplified Source

```c
static int tar_close(Walfile *f, WalCloseMethod method) {
    TarMethodData *tar_data = (TarMethodData *) f->wwmethod;
    TarMethodFile *tf = (TarMethodFile *) f;

    clear_error(f->wwmethod);

    // Handle file unlinking by truncating TAR to start of this file
    if (method == CLOSE_UNLINK) {
        if (f->wwmethod->compression_algorithm != PG_COMPRESSION_NONE) {
            f->wwmethod->lasterrstring = _("unlink not supported with compression");
            return -1;
        }

        if (ftruncate(tar_data->fd, tf->ofs_start) != 0) {
            f->wwmethod->lasterrno = errno;
            return -1;
        }

        // Clean up and exit
        pg_free(tf->base.pathname);
        pg_free(tf);
        tar_data->currentfile = NULL;
        return 0;
    }

    // Handle padding to requested file size
    if (tf->pad_to_size) {
        if (f->wwmethod->compression_algorithm == PG_COMPRESSION_GZIP) {
            // Pad compressed files at close time
            size_t sizeleft = tf->pad_to_size - tf->base.currpos;
            if (sizeleft && !tar_write_padding_data(tf, sizeleft))
                return -1;
        } else {
            // Adjust position for uncompressed files (already padded)
            tf->base.currpos = tf->pad_to_size;
        }
    }

    // Add TAR format padding (align to TAR_BLOCK_SIZE)
    ssize_t filesize = f->currpos;
    int padding = tarPaddingBytesRequired(filesize);
    if (padding) {
        char zerobuf[TAR_BLOCK_SIZE] = {0};
        if (tar_write(f, zerobuf, padding) != padding)
            return -1;
    }

    // Flush compressed data if needed
    if (f->wwmethod->compression_algorithm == PG_COMPRESSION_GZIP) {
        if (!tar_write_compressed_data(tar_data, NULL, 0, true))
            return -1;
    }

    // Update TAR header with final file size and checksum
    print_tar_number(&(tf->header[TAR_OFFSET_SIZE]), 12, filesize);
    if (method == CLOSE_NORMAL)
        strlcpy(&(tf->header[TAR_OFFSET_NAME]), tf->base.pathname, 100);
    print_tar_number(&(tf->header[TAR_OFFSET_CHECKSUM]), 8, tarChecksum(tf->header));

    // Write updated header back to TAR file
    if (lseek(tar_data->fd, tf->ofs_start, SEEK_SET) != tf->ofs_start) {
        f->wwmethod->lasterrno = errno;
        return -1;
    }

    if (f->wwmethod->compression_algorithm == PG_COMPRESSION_NONE) {
        // Write header directly for uncompressed files
        if (write(tar_data->fd, tf->header, TAR_BLOCK_SIZE) != TAR_BLOCK_SIZE) {
            f->wwmethod->lasterrno = errno ? errno : ENOSPC;
            return -1;
        }
    } else if (f->wwmethod->compression_algorithm == PG_COMPRESSION_GZIP) {
        // Handle compressed header updates with temporary compression changes
        deflateParams(tar_data->zp, 0, Z_DEFAULT_STRATEGY);
        tar_write_compressed_data(tar_data, tar_data->currentfile->header, TAR_BLOCK_SIZE, true);
        deflateParams(tar_data->zp, f->wwmethod->compression_level, Z_DEFAULT_STRATEGY);
    }

    // Restore file position to end
    lseek(tar_data->fd, 0, SEEK_END);

    // Always sync on close
    if (tar_sync(f) < 0) {
        pg_fatal("could not fsync file \"%s\": %s",
                tf->base.pathname, GetLastWalMethodError(f->wwmethod));
    }

    // Clean up
    pg_free(tf->base.pathname);
    pg_free(tf);
    tar_data->currentfile = NULL;

    return 0;
}
```