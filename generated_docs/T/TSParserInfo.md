# TSParserInfo

## Location
src/bin/pg_dump/pg_dump.h: 541 - 542

## Overview
TSParserInfo is a structure used in pg_dump to represent a PostgreSQL text search parser, storing metadata needed to dump and restore text search parsers.

## Definition


## Detailed Description
TSParserInfo is part of pg_dump's internal representation of PostgreSQL database objects that need to be dumped and restored. It specifically handles text search parsers, which are components of PostgreSQL's full-text search system. The structure stores the OIDs of the five functions that make up a text search parser: start, token, end, headline, and lextype functions. This information is retrieved from the pg_ts_parser system catalog and used to generate CREATE TEXT SEARCH PARSER statements during database dumps.

## Parameters / Member Variables
- : Base DumpableObject containing common metadata (name, namespace, dump ID, object type)
- : OID of the parser's start function that initializes parsing
- : OID of the parser's token function that extracts the next token
- : OID of the parser's end function that finalizes parsing
- : OID of the parser's headline function for creating search result headlines
- : OID of the parser's lextype function that returns token type information

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [getTSParsers](../g/getTSParsers.md) (populates TSParserInfo structures from pg_ts_parser catalog)
  - [dumpTSParser](../d/dumpTSParser.md) (uses TSParserInfo to generate CREATE TEXT SEARCH PARSER statements)
  - fmtQualifiedDumpable (formats the parser name for output)

## Notes and Other Information
- Located in src/bin/pg_dump/pg_dump.h:533-541
- Used exclusively within pg_dump for backing up and restoring text search parsers
- The structure maps directly to columns in the pg_ts_parser system catalog
- All function OIDs stored in the structure must reference valid PostgreSQL functions that implement the text search parser interface
- Part of PostgreSQL's full-text search infrastructure, which allows custom parsing of text for search indexing