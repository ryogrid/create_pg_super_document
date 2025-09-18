# getOwnedSequences

## Location
src/backend/catalog/pg_depend.c: 937 - 945

## Overview
Collects a list of OIDs for all sequences owned (either identity or serial) by a specified relation, serving as a convenience wrapper function.

## Definition
List *getOwnedSequences(Oid relid)

## Detailed Description
This function serves as a public interface to retrieve all sequences owned by a given relation. It acts as a wrapper around the internal function getOwnedSequences_internal, providing a simplified interface that returns both identity and serial sequences without column-specific filtering. The function is commonly used during table operations that need to identify and handle dependent sequences, such as truncation and rewriting operations.

## Parameters / Member Variables
- `relid`: The OID of the relation (table) for which to find owned sequences

## Dependencies
- Functions called/Symbols referenced:
  - [getOwnedSequences_internal](getOwnedSequences_internal.md)
- Called from (representative examples):
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)
  - [ATRewriteTables](../A/ATRewriteTables.md)

## Notes and Other Information
This is a convenience function that internally calls getOwnedSequences_internal with parameters (relid, 0, 0), which means it retrieves all owned sequences for the relation without filtering by specific columns or sequence types. The function is part of PostgreSQL's dependency management system and is crucial for maintaining referential integrity when performing table operations that affect dependent sequences.