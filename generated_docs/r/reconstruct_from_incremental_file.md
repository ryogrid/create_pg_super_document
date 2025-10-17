# reconstruct_from_incremental_file

## Location
[src/bin/pg_combinebackup/reconstruct.c:88-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L88-L382)

## Overview
Reconstructs a full file from an incremental backup file by traversing a chain of prior backups to gather all necessary blocks.

## Definition

```c
void
reconstruct_from_incremental_file(char *input_filename,
								  char *output_filename,
								  char *relative_path,
								  char *bare_file_name,
								  int n_prior_backups,
								  char **prior_backup_dirs,
								  manifest_data **manifests,
								  char *manifest_path,
								  pg_checksum_type checksum_type,
								  int *checksum_length,
								  uint8 **checksum_payload,
								  CopyMethod copy_method,
								  bool debug,
								  bool dry_run)
```
## Detailed Description
This function is the core of PostgreSQL's incremental backup reconstruction process. It takes an incremental backup file and combines it with blocks from a chain of prior backups to create a complete, reconstructed file. The function implements an intelligent block-sourcing strategy where it first processes the latest incremental file, then traverses backwards through the backup chain to find missing blocks.

The reconstruction process handles both incremental and full files in the backup chain. When a full file is found, it can either be copied entirely (if no blocks from later incrementals are needed) or serve as a source for missing blocks. The function also manages checksum validation and can reuse existing checksums from backup manifests when available.

## Parameters / Member Variables
- `*input_filename`: Path to the incremental file to be reconstructed
- `*output_filename`: Path where the reconstructed full file will be written
- `*relative_path`: Directory path relative to backup root, must end with trailing slash
- `*bare_file_name`: Filename without the "INCREMENTAL." prefix
- `n_prior_backups`: Number of previous backups in the chain
- `**prior_backup_dirs`: Array of pathnames to prior backup directories
- `**manifests`: Array of manifest data structures for checksum validation
- `*manifest_path`: Path to the manifest file for checksum lookup
- `checksum_type`: Type of checksum to calculate for the reconstructed file
- `*checksum_length`: Output parameter for calculated checksum length
- `**checksum_payload`: Output parameter for calculated checksum data
- `copy_method`: Method to use for file copying operations
- `debug`: Flag to enable debug output during reconstruction
- `dry_run`: Flag to perform reconstruction without actually writing files
## Dependencies
- Functions called/Symbols referenced:
  - [make_incremental_rfile](../m/make_incremental_rfile.md)
  - [find_reconstructed_block_length](../f/find_reconstructed_block_length.md)
  - [make_rfile](../m/make_rfile.md)
  - [copy_file](../c/copy_file.md)
  - [write_reconstructed_file](../w/write_reconstructed_file.md)
  - [debug_reconstruction](../d/debug_reconstruction.md)
  - [pg_checksum_init](../p/pg_checksum_init.md)
  - [pg_checksum_final](../p/pg_checksum_final.md)
  - manifest_files_lookup
- Called from (representative examples):
  - [process_directory_recursively](../p/process_directory_recursively.md)

## Notes and Other Information
The function implements sophisticated optimization logic, including the ability to perform full file copies when no blocks from later incrementals are needed. It handles zero-filled blocks that may not be present in any backup due to PostgreSQL's WAL-based incremental backup strategy. The function also includes comprehensive error handling for cases where the backup chain is incomplete or inconsistent.

## Simplified Source

```c
void reconstruct_from_incremental_file(char *input_filename,
                                     char *output_filename,
                                     char *relative_path,
                                     char *bare_file_name,
                                     int n_prior_backups,
                                     char **prior_backup_dirs,
                                     manifest_data **manifests,
                                     char *manifest_path,
                                     pg_checksum_type checksum_type,
                                     int *checksum_length,
                                     uint8 **checksum_payload,
                                     CopyMethod copy_method,
                                     bool debug,
                                     bool dry_run)
{
    rfile **source;
    rfile *latest_source = NULL;
    rfile **sourcemap;
    off_t *offsetmap;
    unsigned block_length;
    unsigned i;
    unsigned sidx = n_prior_backups;
    bool full_copy_possible = true;
    rfile *copy_source = NULL;
    pg_checksum_context checksum_ctx;

    // Allocate arrays for tracking block sources
    source = pg_malloc0(sizeof(rfile *) * (1 + n_prior_backups));

    // Get latest incremental file and determine block length
    latest_source = make_incremental_rfile(input_filename);
    source[n_prior_backups] = latest_source;
    block_length = find_reconstructed_block_length(latest_source);

    // Create mapping arrays for block sources and offsets
    sourcemap = pg_malloc0(sizeof(rfile *) * block_length);
    offsetmap = pg_malloc0(sizeof(off_t) * block_length);

    // Map blocks from latest incremental file
    for (i = 0; i < latest_source->num_blocks; ++i) {
        BlockNumber b = latest_source->relative_block_numbers[i];
        sourcemap[b] = latest_source;
        offsetmap[b] = latest_source->header_length + (i * BLCKSZ);
        full_copy_possible = false;  // Need blocks from incremental
    }

    // Traverse backup chain to find missing blocks
    while (sidx > 0) {
        char source_filename[MAXPGPATH];
        rfile *s;

        --sidx;

        // Try full file first, then incremental
        snprintf(source_filename, MAXPGPATH, "%s/%s%s",
                prior_backup_dirs[sidx], relative_path, bare_file_name);
        s = make_rfile(source_filename, true);
        if (s == NULL) {
            snprintf(source_filename, MAXPGPATH, "%s/%sINCREMENTAL.%s",
                    prior_backup_dirs[sidx], relative_path, bare_file_name);
            s = make_incremental_rfile(source_filename);
        }
        source[sidx] = s;

        if (s->header_length == 0) {
            // Full file found - map all available blocks
            struct stat sb;
            if (fstat(s->fd, &sb) < 0)
                pg_fatal("could not stat file \"%s\": %m", s->filename);

            BlockNumber blocklength = sb.st_size / BLCKSZ;
            for (BlockNumber b = 0; b < latest_source->truncation_block_length; ++b) {
                if (sourcemap[b] == NULL && b < blocklength) {
                    sourcemap[b] = s;
                    offsetmap[b] = b * BLCKSZ;
                }
            }

            // Check if full copy is possible
            if (full_copy_possible) {
                uint64 expected_length = (uint64) latest_source->truncation_block_length * BLCKSZ;
                if (expected_length == sb.st_size) {
                    copy_source = s;
                }
            }
            break;  // No need to check older backups
        } else {
            // Incremental file - map needed blocks
            for (i = 0; i < s->num_blocks; ++i) {
                BlockNumber b = s->relative_block_numbers[i];
                if (b < latest_source->truncation_block_length && sourcemap[b] == NULL) {
                    sourcemap[b] = s;
                    offsetmap[b] = s->header_length + (i * BLCKSZ);
                    full_copy_possible = false;
                }
            }
        }
    }

    // Try to reuse existing checksum from manifest if available
    if (copy_source != NULL && manifests[sidx] != NULL && checksum_type != CHECKSUM_TYPE_NONE) {
        manifest_file *mfile = manifest_files_lookup(manifests[sidx]->files, manifest_path);
        if (mfile != NULL && mfile->checksum_type == checksum_type) {
            *checksum_length = mfile->checksum_length;
            *checksum_payload = pg_malloc(*checksum_length);
            memcpy(*checksum_payload, mfile->checksum_payload, *checksum_length);
            checksum_type = CHECKSUM_TYPE_NONE;
        }
    }

    // Initialize checksum calculation
    pg_checksum_init(&checksum_ctx, checksum_type);

    // Perform reconstruction or copy
    if (copy_source != NULL) {
        // Full copy optimization
        copy_file(copy_source->filename, output_filename, &checksum_ctx, copy_method, dry_run);
    } else if (sidx == 0 && source[0]->header_length != 0) {
        pg_fatal("full backup contains unexpected incremental file \"%s\"", source[0]->filename);
    } else {
        // Block-by-block reconstruction
        write_reconstructed_file(input_filename, output_filename, block_length,
                               sourcemap, offsetmap, &checksum_ctx, copy_method, debug, dry_run);
        debug_reconstruction(n_prior_backups + 1, source, dry_run);
    }

    // Finalize checksum if needed
    if (checksum_type != CHECKSUM_TYPE_NONE) {
        *checksum_payload = pg_malloc(PG_CHECKSUM_MAX_LENGTH);
        *checksum_length = pg_checksum_final(&checksum_ctx, *checksum_payload);
    }

    // Cleanup resources
    for (i = 0; i <= n_prior_backups; ++i) {
        rfile *s = source[i];
        if (s != NULL) {
            close(s->fd);
            if (s->relative_block_numbers != NULL)
                pfree(s->relative_block_numbers);
            pg_free(s->filename);
        }
    }
    pfree(sourcemap);
    pfree(offsetmap);
    pfree(source);
}
```