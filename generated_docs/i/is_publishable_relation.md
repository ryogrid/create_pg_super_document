# is_publishable_relation

## Location
src/backend/catalog/pg_publication.c: 150 - 162

## Overview
A convenience wrapper function that determines if an opened relation is publishable by delegating to `is_publishable_class()`.

## Definition
```c
bool is_publishable_relation(Relation rel)
```

## Detailed Description
This function is a simple wrapper around `is_publishable_class()` designed for situations where you already have an opened Relation object. It extracts the necessary information from the Relation structure (OID and Form_pg_class tuple) and passes them to `is_publishable_class()` for the actual publishability determination.

This function provides a more convenient interface when working with opened relations, as it eliminates the need for callers to manually extract `RelationGetRelid(rel)` and `rel->rd_rel` parameters.

The publishability criteria are the same as `is_publishable_class()`:
- Must be a regular or partitioned table
- Cannot be a catalog relation
- Must have permanent persistence (not temporary or unlogged)
- Must have been created after initdb

## Parameters / Member Variables
- `rel`: A Relation pointer to an opened relation object to check for publishability

## Dependencies
- Functions called/Symbols referenced:
  - is_publishable_class
  - RelationGetRelid (macro to extract OID from relation)
- Called from:
  - pgoutput_change (in replication output plugin)
  - pgoutput_truncate (in replication output plugin)
  - RelationBuildPublicationDesc (in relation cache)
  - PublicationPartOpt (macro/inline function)

## Notes and Other Information
- This is a non-static function, making it accessible from other compilation units
- Unlike the check_* functions, this returns a boolean rather than throwing errors
- Serves as a convenience wrapper to avoid code duplication in callers
- Commonly used in replication contexts where relations are already opened
- The function signature is declared in src/include/catalog/pg_publication.h
- Very lightweight - just extracts relation metadata and delegates to the core logic
- Location: src/backend/catalog/pg_publication.c:150-162