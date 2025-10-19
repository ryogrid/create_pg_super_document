# rewriteVisibilityMap

## Location
[src/bin/pg_upgrade/file.c:216-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/file.c#L216-L359)

## Overview
Converts old-format visibility map files to the new format during PostgreSQL upgrades, transforming single-bit-per-page visibility maps to two-bit-per-page format while preserving existing visibility information.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
The rewriteVisibilityMap function performs a critical transformation during PostgreSQL upgrades when migrating from versions prior to catversion 201603011 (PostgreSQL 9.6) to newer versions. In older PostgreSQL versions, visibility maps used one bit per heap page to track all-visible pages. Modern PostgreSQL uses two bits per page: one for all-visible and one for all-frozen status.

The function reads the old single-bit visibility map file page by page and converts each page into potentially two new-format pages. For each old byte (representing 8 heap pages), it creates new 16-bit values where the old all-visible bit is preserved in the all-visible position of the new format. The all-frozen bits are left unset, allowing future VACUUM operations to set them appropriately.

The conversion process maintains the original page headers and handles edge cases like partial pages and empty trailing sections. If checksums are enabled in the new cluster, appropriate checksums are calculated for the new visibility map pages.

## Parameters / Member Variables
- : Path to the source old-format visibility map file
- : Path to the destination new-format visibility map file
- : SQL schema name of the relation (used only for error reporting)
- : SQL relation name (used only for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - open
  - fstat
  - read
  - write
  - close
  - memcpy
  - [pg_checksum_page](../p/pg_checksum_page.md)
  - [pg_fatal](../p/pg_fatal.md)
  - PGIOAlignedBlock
  - [PageHeaderData](../P/PageHeaderData.md)
  - PageHeader
  - SizeOfPageHeaderData
  - BLCKSZ
  - BITS_PER_BYTE
  - BITS_PER_HEAPBLOCK
  - VISIBILITYMAP_ALL_VISIBLE
  - PG_BINARY
  - pg_file_create_mode
- Called from (representative examples):
  - [transfer_relfile](../t/transfer_relfile.md)

## Notes and Other Information
- Only needed when upgrading from PostgreSQL versions before 9.6 that used single-bit visibility maps
- Converts one old visibility map page into potentially two new pages due to the expanded bit format
- Preserves all-visible information but does not set all-frozen bits (left for future VACUUM)
- Handles partial pages and empty trailing sections to avoid unnecessary page expansion
- Calculates checksums for new pages if data checksums are enabled in the target cluster
- The conversion maintains the benefit of existing visibility information, avoiding the need to re-scan entire tables
- Uses aligned I/O buffers (PGIOAlignedBlock) for optimal performance
- Each old byte (8 heap pages) becomes 16 bits (8 × 2 bits per page) in the new format
- Critical for maintaining performance during major version upgrades by preserving visibility optimization data

## Simplified Source

```c
void rewriteVisibilityMap(const char *fromfile, const char *tofile,
                         const char *schemaName, const char *relName) {
    int src_fd, dst_fd;
    PGIOAlignedBlock buffer, new_vmbuf;
    ssize_t totalBytesRead = 0, src_filesize;
    int rewriteVmBytesPerPage;
    BlockNumber new_blkno = 0;
    struct stat statbuf;

    // Calculate how many old-format bytes fit per new page
    rewriteVmBytesPerPage = (BLCKSZ - SizeOfPageHeaderData) / 2;

    // Open source file and get its size
    if ((src_fd = open(fromfile, O_RDONLY | PG_BINARY, 0)) < 0)
        pg_fatal("error while copying relation \"%s.%s\": could not open file \"%s\": %m",
                 schemaName, relName, fromfile);

    if (fstat(src_fd, &statbuf) != 0)
        pg_fatal("error while copying relation \"%s.%s\": could not stat file \"%s\": %m",
                 schemaName, relName, fromfile);

    // Create destination file
    if ((dst_fd = open(tofile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
                       pg_file_create_mode)) < 0)
        pg_fatal("error while copying relation \"%s.%s\": could not create file \"%s\": %m",
                 schemaName, relName, tofile);

    src_filesize = statbuf.st_size;

    // Process each old page and convert to new format
    while (totalBytesRead < src_filesize) {
        ssize_t bytesRead;
        char *old_cur, *old_break, *old_blkend;
        PageHeaderData pageheader;
        bool old_lastblk;

        // Read one old page
        if ((bytesRead = read(src_fd, buffer.data, BLCKSZ)) != BLCKSZ) {
            if (bytesRead < 0)
                pg_fatal("error while copying relation \"%s.%s\": could not read file \"%s\": %m",
                         schemaName, relName, fromfile);
            else
                pg_fatal("error while copying relation \"%s.%s\": partial page found in file \"%s\"",
                         schemaName, relName, fromfile);
        }

        totalBytesRead += BLCKSZ;
        old_lastblk = (totalBytesRead == src_filesize);

        // Save page header and set up pointers
        memcpy(&pageheader, buffer.data, SizeOfPageHeaderData);
        old_cur = buffer.data + SizeOfPageHeaderData;
        old_blkend = buffer.data + bytesRead;
        old_break = old_cur + rewriteVmBytesPerPage;

        // Convert old page data to new format page(s)
        while (old_break <= old_blkend) {
            char *new_cur;
            bool empty = true;
            bool old_lastpart;

            // Copy page header to new page
            memcpy(new_vmbuf.data, &pageheader, SizeOfPageHeaderData);
            old_lastpart = old_lastblk && (old_break == old_blkend);
            new_cur = new_vmbuf.data + SizeOfPageHeaderData;

            // Convert each old byte to new 2-bit format
            while (old_cur < old_break) {
                uint8 byte = *(uint8 *) old_cur;
                uint16 new_vmbits = 0;
                int i;

                // Convert 8 single bits to 8 double bits
                for (i = 0; i < BITS_PER_BYTE; i++) {
                    if (byte & (1 << i)) {
                        empty = false;
                        new_vmbits |= VISIBILITYMAP_ALL_VISIBLE << (BITS_PER_HEAPBLOCK * i);
                    }
                }

                // Store new format bits
                new_cur[0] = (char) (new_vmbits & 0xFF);
                new_cur[1] = (char) (new_vmbits >> 8);

                old_cur++;
                new_cur += BITS_PER_HEAPBLOCK;
            }

            // Skip empty trailing sections
            if (old_lastpart && empty)
                break;

            // Add checksum if enabled
            if (new_cluster.controldata.data_checksum_version != 0)
                ((PageHeader) new_vmbuf.data)->pd_checksum =
                    pg_checksum_page(new_vmbuf.data, new_blkno);

            // Write new page
            errno = 0;
            if (write(dst_fd, new_vmbuf.data, BLCKSZ) != BLCKSZ) {
                if (errno == 0)
                    errno = ENOSPC;
                pg_fatal("error while copying relation \"%s.%s\": could not write file \"%s\": %m",
                         schemaName, relName, tofile);
            }

            // Advance to next section
            old_break += rewriteVmBytesPerPage;
            new_blkno++;
        }
    }

    // Clean up
    close(dst_fd);
    close(src_fd);
}
```