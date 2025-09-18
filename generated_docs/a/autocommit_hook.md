# autocommit_hook

## Location
src/bin/psql/startup.c: 875 - 880

## Overview
An assignment hook function for the AUTOCOMMIT psql variable that validates and sets the autocommit behavior in psql's global settings.

## Definition


## Detailed Description
This function serves as an assignment hook in psql's variable management system, specifically responsible for handling changes to the AUTOCOMMIT variable. When the AUTOCOMMIT variable is modified via \set commands, this hook validates the new boolean value and updates the corresponding field in psql's global settings structure (pset.autocommit). The function leverages PostgreSQL's standard boolean parsing utilities to ensure consistent validation and error handling.

## Parameters / Member Variables
- : The new string value being assigned to the AUTOCOMMIT variable

## Dependencies
- Functions called/Symbols referenced:
  - ParseVariableBool (boolean value parsing and validation)
- Called from (representative examples):
  - EstablishVariableSpace (variable registration during initialization)

## Notes and Other Information
- Part of psql's variable hook system that ensures special variables have proper validation and side effects
- Returns true on successful parsing and assignment, false on invalid input
- The AUTOCOMMIT setting controls whether psql automatically commits transactions or requires explicit commit commands
- Integrates with the global pset structure that maintains psql's runtime configuration
- Error handling for invalid boolean values is managed by ParseVariableBool
- This hook ensures that changes to the AUTOCOMMIT variable immediately affect psql's transaction behavior