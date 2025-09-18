# isTempToastNamespace

## Location
src/backend/catalog/namespace.c: 3661 - 3672

## Overview
Determines whether a given namespace OID corresponds to the current session's temporary TOAST table namespace.

## Definition


## Detailed Description
The isTempToastNamespace function checks if the provided namespace OID matches the current session's temporary TOAST namespace. PostgreSQL uses TOAST (The Oversized-Attribute Storage Technique) to store large field values that exceed the page size limit. When temporary tables are created that require TOAST storage, PostgreSQL creates a separate temporary TOAST namespace to house the TOAST tables and indexes associated with temporary tables.

This function provides a way to identify whether a given namespace is the calling session's temporary TOAST namespace by comparing the input OID with the global variable myTempToastNamespace. Like its counterpart isTempNamespace, it first validates the OID before performing the comparison.

## Parameters / Member Variables
- : The OID of the namespace to check against the current session's temporary TOAST namespace.

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid: Validates that myTempToastNamespace contains a valid OID
  - myTempToastNamespace: Global variable storing the current session's temporary TOAST namespace OID

- Called from (representative examples):
  - IsToastNamespace: Used to determine if a namespace is any kind of TOAST namespace (regular or temporary)
  - RangeVarGetRelid: During relation name resolution involving temporary TOAST objects

## Notes and Other Information
- This function is session-specific and only identifies the calling session's temporary TOAST namespace
- Returns false if myTempToastNamespace is invalid or if the provided namespaceId doesn't match
- The temporary TOAST namespace is created automatically when temporary tables requiring TOAST storage are first created in a session
- TOAST tables for temporary tables are stored separately from regular TOAST tables to maintain proper isolation and cleanup semantics
- The myTempToastNamespace variable may be InvalidOid if no temporary tables requiring TOAST have been created in the current session
- This function is used primarily for namespace classification and special handling of temporary TOAST objects in the PostgreSQL system