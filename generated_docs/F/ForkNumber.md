# ForkNumber

## Location
src/include/common/relpath.h: 60 - 61

## Overview
ForkNumber is an enumeration type that identifies different forks (storage files) within a single PostgreSQL relation, allowing the database to manage multiple physical files per logical relation for different purposes.

## Definition

Located at src/include/common/relpath.h:47-60

## Detailed Description
ForkNumber is a crucial component of PostgreSQL's storage architecture that enables a single logical relation (table or index) to have multiple physical storage files, each serving a specific purpose. The physical storage of a relation consists of one or more forks, where the main fork is always created, but additional forks store various metadata and auxiliary information.

Each fork serves a distinct purpose:
- **MAIN_FORKNUM (0)**: Contains the actual table or index data
- **FSM_FORKNUM (1)**: Free Space Map - tracks available space in data pages for efficient insertion
- **VISIBILITYMAP_FORKNUM (2)**: Visibility Map - tracks which pages contain only tuples visible to all transactions (used for VACUUM optimization)
- **INIT_FORKNUM (3)**: Initialization fork - used for unlogged tables to track initialization status

This design allows PostgreSQL to efficiently manage storage metadata alongside the actual data, optimizing operations like VACUUM, INSERT, and visibility checks.

## Parameters / Member Variables
- : Represents an invalid or uninitialized fork number
- : The primary data fork containing actual relation data
- : Free Space Map fork for tracking available space in pages  
- : Visibility Map fork for MVCC optimization
- : Initialization fork for unlogged table management

## Dependencies
- Functions called/Symbols referenced:
  - Used with RelFileNumber to fully specify relation files
  - MAX_FORKNUM constant (set to INIT_FORKNUM)
  - FORKNAMECHARS constant (maximum 4 characters for fork names)

- Called from (representative examples):
  - forkNames[] array (maps fork numbers to string names: "main", "fsm", "vm", "init")
  - [forkname_to_number](../f/forkname_to_number.md)() function (converts fork name strings to ForkNumber)
  - [forkname_chars](../f/forkname_chars.md)() function (parses fork names from filenames)
  - Buffer management system (BufferTag structure)
  - Storage path construction (GetRelationPath function)

## Notes and Other Information
- The forkNames array in src/common/relpath.c provides string representations: ["main", "fsm", "vm", "init"]
- When adding new fork types, developers must update MAX_FORKNUM, FORKNAMECHARS, forkNames array, and documentation
- Fork files are physically stored with suffixes (_fsm, _vm, _init) appended to the base relation filename
- Critical for PostgreSQL's MVCC implementation and space management optimization
- Used extensively in buffer management, WAL logging, and backup/recovery operations
- The InvalidForkNumber value (-1) is used to indicate error conditions or uninitialized states