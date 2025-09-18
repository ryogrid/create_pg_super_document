# RelationMapInitialize

## Location
src/backend/utils/cache/relmapper.c: 651 - 670

## Overview
RelationMapInitialize initializes the relation mapper module at process startup, ensuring all mapping structures are reset to empty states before database access begins.

## Definition


## Detailed Description
This function performs the initial setup of the relation mapper system during PostgreSQL process startup. It explicitly initializes all static mapping variables to zero states, clearing any potential residual data. The function ensures that both shared and local relation maps start in a clean state, with no active mappings or pending updates. This initialization occurs before database access is available, making it a critical early-stage setup function.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced: None (only accesses static variables)
- Called from (representative examples):
  - RelationCacheInitialize (at src/backend/utils/cache/relcache.c:4028)

## Notes and Other Information
- This function explicitly zeros out static variables even though they should initialize to zero by default, ensuring deterministic behavior
- Initializes both shared_map and local_map structures along with their associated update tracking structures
- Must be called before any database access occurs during process startup
- Part of the relation mapping system that handles OID-to-filenode mappings for critical system relations