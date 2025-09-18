# pg_enc2name

## Location
src/include/mb/pg_wchar.h: 355 - 362

## Overview
A structure that maps PostgreSQL encoding numbers to their official encoding names and platform-specific data, serving as the fundamental data structure for encoding identification and conversion within PostgreSQL's multi-byte character support system.

## Definition


## Detailed Description
The  structure is the core data structure used in PostgreSQL's encoding management system. It provides a mapping between PostgreSQL's internal encoding identifiers ( enum values) and their corresponding official encoding names as strings. This structure is primarily used to populate the  array, which serves as the authoritative lookup table for encoding information throughout the PostgreSQL system.

The structure is designed to be accessed through the  macro check to ensure safe array bounds access. On Windows platforms, it additionally stores the system codepage number to facilitate proper character encoding conversion with the Windows API.

This structure is essential for PostgreSQL's internationalization support, enabling the database to work with various character encodings while maintaining consistent internal representation and conversion capabilities.

## Parameters / Member Variables
- : A constant string pointer containing the official name of the character encoding (e.g., "UTF8", "LATIN1", "EUC_JP")
- : The PostgreSQL internal encoding identifier of type , which serves as an index into the encoding tables
- 
Usage: cpi code_page_file [-c] [-L] [-l] [-a|nnn]
 -c: input file is a single codepage
 -L: print header info (you don't want to see this)
 -l or no option: list all codepages contained in the file
 -a: extract all codepages from the file
 nnn (3 digits): extract codepage nnn from the file
Example: cpi ega.cpi 850 
 will create a file 850.cp containing the requested codepage.: (Windows only) An unsigned integer representing the Windows system codepage number corresponding to this encoding, used for Windows-specific character conversion operations

## Dependencies
- Functions called/Symbols referenced:
  - pg_enc (enum type for encoding identifiers)
- Called from (representative examples):
  - pg_encoding_to_char (in src/common/encnames.c:591)
  - Used in pg_enc2name_tbl[] array for encoding lookups

## Notes and Other Information
- The structure must be carefully validated using  before accessing table entries to prevent buffer overflows
- The structure is part of PostgreSQL's core multi-byte character support system located in src/include/mb/pg_wchar.h
- When adding new encodings, developers must update the corresponding , , and  arrays
- The Windows-specific codepage member is conditionally compiled and only present on WIN32 builds
- This structure is fundamental to PostgreSQL's ability to support international character sets and is used extensively throughout the encoding conversion subsystem