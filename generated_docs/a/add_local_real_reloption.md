# add_local_real_reloption

## Location
[src/backend/access/common/reloptions.c:972-988](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L972-L988)

## Overview
The add_local_real_reloption function adds a new local floating-point reloption (relation option) with specified validation constraints and default value.

## Definition
void add_local_real_reloption(local_relopts *relopts, const char *name, const char *desc, double default_val, double min_val, double max_val, int offset)

## Detailed Description
This function creates and registers a new local floating-point relation option within the PostgreSQL reloptions system. It serves as a wrapper that first initializes a real-type reloption using init_real_reloption() with the RELOPT_KIND_LOCAL kind, then adds it to the local reloptions structure. The function is part of PostgreSQL's extensible relation options framework that allows custom data types and access methods to define their own configuration parameters.

## Parameters / Member Variables
- : Pointer to the local_relopts structure where the new option will be added
- : String name identifier for the reloption
- : Human-readable description of the option's purpose
- : Default floating-point value if not explicitly set
- : Minimum allowed value for validation
- : Maximum allowed value for validation
- : Byte offset of the double-typed field in the target structure

## Dependencies
- Functions called/Symbols referenced:
  - [init_real_reloption](../i/init_real_reloption.md)
  - [add_local_reloption](add_local_reloption.md)
  - RELOPT_KIND_LOCAL
  - [relopt_real](../r/relopt_real.md)
  - [relopt_gen](../r/relopt_gen.md)
- Called from (representative examples):
  - [brin_bloom_options](../b/brin_bloom_options.md) (in BRIN bloom index access method)

## Notes and Other Information
- This function is specifically designed for local (non-global) relation options
- The offset parameter must correspond to a double-typed field in the target structure
- Part of the broader reloptions infrastructure that supports extensible table and index parameters
- Used primarily by access method implementations that need floating-point configuration options