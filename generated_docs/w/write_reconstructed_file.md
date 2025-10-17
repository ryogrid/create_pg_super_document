# write_reconstructed_file

## Location
[src/bin/pg_combinebackup/reconstruct.c:551-750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L551-L750)

## Overview
A core function in the pg_combinebackup utility that reconstructs and writes a complete file from multiple incremental backup sources, handling block-by-block reconstruction with optional dry-run and debugging capabilities.

## Definition

```c
static void
write_reconstructed_file(char *input_filename,
						 char *output_filename,
						 unsigned block_length,
						 rfile **sourcemap,
						 off_t *offsetmap,
						 pg_checksum_context *checksum_ctx,
						 CopyMethod copy_method,
						 bool debug,
						 bool dry_run)
```
## Detailed Description
The  function is the main workhorse for file reconstruction in PostgreSQL's incremental backup system. It takes a mapping of source files and block offsets and reconstructs a complete output file by reading blocks from various sources or zero-filling them when needed.

The function supports multiple copy methods including standard read/write operations and the more efficient  system call when available. It provides comprehensive debugging output showing the reconstruction plan and tracks statistics about blocks read from each source. The function also handles checksum calculation for the reconstructed file and supports a dry-run mode for planning purposes.

## Parameters / Member Variables
- `*input_filename`: Name of the input file being processed (used primarily for error messages)
- `*output_filename`: Path where the reconstructed file will be written
- `block_length`: Total number of blocks in the reconstructed file
- `**sourcemap`: Array mapping each block index to its source rfile structure (NULL for zero-filled blocks)
- `*offsetmap`: Array of file offsets corresponding to each block's location in its source file
- `*checksum_ctx`: Context for checksum calculation during file reconstruction
- `copy_method`: Method to use for copying data (standard copy vs copy_file_range)
- `debug`: Flag to enable detailed debugging output showing reconstruction plan
- `dry_run`: Flag to simulate reconstruction without actually creating output file
## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug (for debugging output)
  - pg_checksum_type_name (for checksum type display)
  - open (to create output file)
  - write_block (to write individual blocks)
  - read_block (to read blocks from source files)
  - copy_file_range (efficient block copying when available)
  - pg_checksum_update (for checksum calculation)
  - close (to close output file)

- Called from:
  - reconstruct_from_incremental_file (main reconstruction workflow)

## Notes and Other Information
- This is a static function within the pg_combinebackup reconstruction module
- Supports two copy methods: traditional read/write and copy_file_range for better performance
- Zero-fills blocks that aren't present in any source file (new/uninitialized blocks)
- Provides detailed debugging output showing the reconstruction plan by block ranges
- Tracks statistics for each source file including blocks read and highest offset accessed
- Uses PostgreSQL's standard error reporting with pg_fatal for unrecoverable errors
- The dry_run mode allows previewing the reconstruction process without file creation
- Handles platform differences gracefully (copy_file_range availability)
- Block size is assumed to be BLCKSZ (typically 8KB in PostgreSQL)

## Simplified Source

```c
static void write_reconstructed_file(char *input_filename,
                                   char *output_filename,
                                   unsigned block_length,
                                   rfile **sourcemap,
                                   off_t *offsetmap,
                                   pg_checksum_context *checksum_ctx,
                                   CopyMethod copy_method,
                                   bool debug,
                                   bool dry_run)
{
    int wfd = -1;
    unsigned i;
    unsigned zero_blocks = 0;

    // Debug output showing reconstruction plan
    if (debug) {
        StringInfoData debug_buf;
        unsigned start_of_range = 0;
        unsigned current_block = 0;

        if (dry_run)
            pg_log_debug("would reconstruct \"%s\" (%u blocks, checksum %s)",
                        output_filename, block_length,
                        pg_checksum_type_name(checksum_ctx->type));
        else
            pg_log_debug("reconstructing \"%s\" (%u blocks, checksum %s)",
                        output_filename, block_length,
                        pg_checksum_type_name(checksum_ctx->type));

        // Show reconstruction plan by block ranges
        initStringInfo(&debug_buf);
        while (current_block < block_length) {
            rfile *s = sourcemap[current_block];

            // Extend range if same source
            if (current_block + 1 < block_length && s == sourcemap[current_block + 1]) {
                ++current_block;
                continue;
            }

            // Add range details to debug output
            if (s == NULL) {
                if (current_block == start_of_range)
                    appendStringInfo(&debug_buf, " %u:zero", current_block);
                else
                    appendStringInfo(&debug_buf, " %u-%u:zero", start_of_range, current_block);
            } else {
                if (current_block == start_of_range)
                    appendStringInfo(&debug_buf, " %u:%s@" UINT64_FORMAT,
                                   current_block, s->filename, (uint64) offsetmap[current_block]);
                else
                    appendStringInfo(&debug_buf, " %u-%u:%s@" UINT64_FORMAT,
                                   start_of_range, current_block, s->filename,
                                   (uint64) offsetmap[current_block]);
            }

            start_of_range = ++current_block;

            // Dump debug output if it gets long
            if (current_block == block_length || debug_buf.len > 1024) {
                pg_log_debug("reconstruction plan:%s", debug_buf.data);
                resetStringInfo(&debug_buf);
            }
        }
        pfree(debug_buf.data);
    }

    // Open output file (skip in dry-run mode)
    if (!dry_run &&
        (wfd = open(output_filename, O_RDWR | PG_BINARY | O_CREAT | O_EXCL,
                   pg_file_create_mode)) < 0)
        pg_fatal("could not open file \"%s\": %m", output_filename);

    // Process each block
    for (i = 0; i < block_length; ++i) {
        uint8 buffer[BLCKSZ];
        rfile *s = sourcemap[i];

        // Update accounting
        if (s == NULL) {
            ++zero_blocks;
        } else {
            s->num_blocks_read++;
            s->highest_offset_read = Max(s->highest_offset_read, offsetmap[i] + BLCKSZ);
        }

        if (dry_run)
            continue;

        // Handle zero-filled blocks
        if (s == NULL) {
            memset(buffer, 0, BLCKSZ);
            write_block(wfd, output_filename, buffer, checksum_ctx);
            continue;
        }

        // Copy block using specified method
        if (copy_method != COPY_METHOD_COPY_FILE_RANGE) {
            // Standard read/write method
            read_block(s, offsetmap[i], buffer);
            write_block(wfd, output_filename, buffer, checksum_ctx);
        } else {
            // Use copy_file_range for efficiency
#if defined(HAVE_COPY_FILE_RANGE)
            off_t off = offsetmap[i];
            size_t nwritten = 0;

            do {
                int wb = copy_file_range(s->fd, &off, wfd, NULL, BLCKSZ - nwritten, 0);
                if (wb < 0)
                    pg_fatal("error while copying file range from \"%s\" to \"%s\": %m",
                            input_filename, output_filename);
                nwritten += wb;
            } while (BLCKSZ > nwritten);

            // Update checksum if needed
            if (checksum_ctx->type != CHECKSUM_TYPE_NONE) {
                read_block(s, offsetmap[i], buffer);
                if (pg_checksum_update(checksum_ctx, buffer, BLCKSZ) < 0)
                    pg_fatal("could not update checksum of file \"%s\"", output_filename);
            }
#else
            pg_fatal("copy_file_range not supported on this platform");
#endif
        }
    }

    // Debug output for zero-filled blocks
    if (zero_blocks > 0) {
        if (dry_run)
            pg_log_debug("would have zero-filled %u blocks", zero_blocks);
        else
            pg_log_debug("zero-filled %u blocks", zero_blocks);
    }

    // Close output file
    if (wfd >= 0 && close(wfd) != 0)
        pg_fatal("could not close file \"%s\": %m", output_filename);
}
```