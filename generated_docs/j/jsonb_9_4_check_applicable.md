# jsonb_9_4_check_applicable

## Location
src/bin/pg_upgrade/version.c: 21 - 36

## Overview
A version hook function that determines whether a JSONB data type compatibility check should be executed during PostgreSQL cluster upgrades.

## Definition


## Detailed Description
This function is part of the pg_upgrade utility's version-specific compatibility checking system. It specifically targets PostgreSQL 9.4 clusters where the JSONB storage format changed during the beta phase. The function checks if the source cluster is running a version of PostgreSQL 9.4 that predates the JSONB format change, which would require special handling during the upgrade process.

The function examines the cluster's major version and catalog version to determine if JSONB data might be stored in the old format that is incompatible with newer versions.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure containing metadata about the PostgreSQL cluster being examined, including version information and control data

## Dependencies
- Functions called/Symbols referenced:
  - GET_MAJOR_VERSION (macro for extracting major version number)
  - JSONB_FORMAT_CHANGE_CAT_VER (constant defining the catalog version when JSONB format changed)
- Called from (representative examples):
  - ALL_VERSIONS (referenced in check.c for version-specific checks)

## Notes and Other Information
- This function specifically targets the JSONB storage format change that occurred during PostgreSQL 9.4 beta development
- Returns true only for PostgreSQL 9.4 clusters with catalog versions prior to the JSONB format change
- Part of the pg_upgrade utility's comprehensive data type compatibility checking framework
- The catalog version comparison ensures precise detection of clusters that need JSONB migration handling