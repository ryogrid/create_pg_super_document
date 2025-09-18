# VersionedQuery

## Location
src/bin/psql/tab-complete.c: 100 - 104

## Overview
VersionedQuery is a struct used in PostgreSQL's psql tab completion system to handle server version-dependent query variations, allowing different SQL queries to be used based on the minimum PostgreSQL server version required.

## Definition


## Detailed Description
The VersionedQuery structure is part of psql's tab completion infrastructure that enables version-aware query execution. It addresses the challenge where tab completion queries must vary depending on the PostgreSQL server version being connected to. The system stores arrays of VersionedQuery entries, each tagged with the minimum server version required for that specific query to work properly.

When psql needs to perform tab completion, it traverses through the VersionedQuery array to find the first query that is compatible with the current server version. Arrays are stored in descending server version order, ensuring that the most recent (and presumably most feature-rich) query that works with the current server is selected first.

The array is terminated with an entry having min_server_version = 0, which can contain either a fallback query that works with all older supported server versions, or NULL to indicate that no completion should be attempted for very old servers.

## Parameters / Member Variables
- : Integer representing the minimum PostgreSQL server version (in numeric format, e.g., 100000 for version 10.0) required for the associated query to work correctly
- : Const char pointer to the SQL query string to be executed for tab completion, or NULL to indicate no completion should be performed

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure)
- Called from (representative examples):
  - [complete_from_versioned_query](../c/complete_from_versioned_query.md) (uses VersionedQuery arrays)
  - Query_for_list_of_publications (static array of VersionedQuery)
  - Query_for_list_of_subscriptions (static array of VersionedQuery)

## Notes and Other Information
- Used extensively in src/bin/psql/tab-complete.c for implementing server version-aware tab completion
- Arrays must be sorted in descending order by min_server_version
- The completion system compares pset.sversion (current server version) against min_server_version to select appropriate queries
- Provides a clean abstraction for handling PostgreSQL's evolution where newer versions introduce new system catalogs, columns, or SQL syntax
- The infrastructure includes test coverage in t/010_tab_completion.pl to verify VersionedQuery functionality