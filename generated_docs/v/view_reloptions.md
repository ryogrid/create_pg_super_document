# view_reloptions

## Location
[src/backend/access/common/reloptions.c:2007-2027](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L2007-L2027)

## Overview
Parses and validates relation options specifically for database views, handling view-specific options like security barriers, security invokers, and check options.

## Definition

```c
bytea *
view_reloptions(Datum reloptions, bool validate)
```
## Detailed Description
The `view_reloptions` function is a specialized option parser for PostgreSQL views that processes view-specific relation options. It defines and processes three key view options: `security_barrier`, `security_invoker`, and `check_option`. The function uses the generic `build_reloptions` infrastructure to parse and validate the options, ensuring they conform to the expected types and values for view configurations. This function is part of PostgreSQL's reloptions system that allows fine-grained control over database object behavior through customizable options.

## Parameters / Member Variables
- `reloptions`: Datum containing the raw relation options to be parsed and processed
- `validate`: Boolean flag indicating whether to perform validation of the option values during parsing

## Dependencies
- Functions called/Symbols referenced:
  - [build_reloptions](../b/build_reloptions.md)
  - relopt_parse_elt (structure)
  - RELOPT_TYPE_BOOL (constant)
  - RELOPT_TYPE_ENUM (constant) 
  - RELOPT_KIND_VIEW (constant)
  - ViewOptions (structure)
  - lengthof (macro)
- Called from (representative examples):
  - [extractRelOptions](../e/extractRelOptions.md)
  - [DefineRelation](../D/DefineRelation.md)
  - [ATExecSetRelOptions](../A/ATExecSetRelOptions.md)

## Notes and Other Information
- The function defines three view-specific options in a static parsing table: security_barrier (boolean), security_invoker (boolean), and check_option (enum)
- Uses the standard reloptions parsing infrastructure via build_reloptions with RELOPT_KIND_VIEW
- Returns a bytea structure containing the parsed ViewOptions, which can be stored and retrieved as needed
- The security_barrier option controls whether the view acts as a security barrier for row-level security
- The security_invoker option determines whether the view executes with the privileges of the invoker or definer
- The check_option controls the behavior of INSERT/UPDATE operations on updatable views