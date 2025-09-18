# PGChecksummablePage

## Location
src/include/storage/checksum_impl.h: 115 - 134

## Overview
A union data structure that provides safe aliasing for PostgreSQL page data, allowing access both as page header information and as a multi-dimensional uint32 array for efficient checksum computation.

## Definition
typedef union
{
    PageHeaderData phdr;
    uint32 data[BLCKSZ / (sizeof(uint32) * N_SUMS)][N_SUMS];
} PGChecksummablePage;

## Detailed Description
PGChecksummablePage is a carefully designed union that solves the strict aliasing problem when accessing PostgreSQL page data for checksum calculations. The union allows the same memory to be interpreted either as a standard PageHeaderData structure (for normal page operations) or as a two-dimensional uint32 array optimized for the parallel checksum algorithm. This design ensures that the checksum computation can efficiently process page data in 32 parallel streams while maintaining type safety and compliance with C's strict aliasing rules.

The data array dimension calculation ensures that the entire BLCKSZ page is covered by the checksum algorithm, with the data organized into chunks that can be processed by N_SUMS parallel hash computations.

## Parameters / Member Variables
- : PageHeaderData structure providing access to standard page header fields including pd_checksum, pd_lsn, and other page metadata
- : Two-dimensional uint32 array with dimensions [BLCKSZ/(sizeof(uint32)*N_SUMS)][N_SUMS], organizing page content for parallel checksum processing

## Dependencies
- Functions called/Symbols referenced:
  - PageHeaderData (standard page header structure)
  - BLCKSZ (system constant for block size)
  - N_SUMS (constant defining parallel checksum streams)
- Called from (representative examples):
  - pg_checksum_block (uses data array for checksum computation)
  - pg_checksum_page (casts page pointer to this type)

## Notes and Other Information
- Union design ensures strict aliasing compliance while enabling efficient checksum computation
- Size is guaranteed to equal BLCKSZ through assertion checks in checksum functions
- The data array layout is specifically designed for the 32-way parallel FNV-1a hash algorithm
- Provides type-safe access to both page metadata and raw page content for checksumming
- Critical component of PostgreSQL's page integrity verification system