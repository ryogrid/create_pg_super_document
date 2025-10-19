# macaddr_trunc

## Location
[src/backend/utils/adt/mac.c:341-362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L341-L362)

## Overview
Truncates a MAC address to keep only the manufacturer identifier (first 3 bytes), setting the device-specific portion to zero for manufacturer comparison purposes.

## Definition

```c
Datum
macaddr_trunc(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements MAC address truncation for PostgreSQL's  data type. It extracts the manufacturer identifier from a MAC address by preserving the first three bytes (a, b, c) which represent the Organizationally Unique Identifier (OUI) assigned by IEEE, and zeroing out the last three bytes (d, e, f) which represent the device-specific portion. This function was implemented based on a suggestion by Alex Pilosov and is commonly used for comparing MAC addresses by manufacturer rather than individual device.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument:  - The MAC address to truncate

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts macaddr argument from function call
  -  - PostgreSQL memory allocation function  
  -  - Returns macaddr result following PostgreSQL conventions
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Preserves only the manufacturer portion (OUI) of the MAC address - bytes a, b, c
- Sets the device-specific portion to zero - bytes d, e, f become 0x00
- Useful for grouping or comparing devices by manufacturer
- Memory for the result is allocated in the current memory context
- This function follows PostgreSQL's V1 calling convention for built-in functions
- The OUI (first 3 bytes) uniquely identifies the manufacturer/vendor of the network interface

## Simplified Source

```c
Datum macaddr_trunc(PG_FUNCTION_ARGS) {
    // Extract input MAC address
    macaddr *addr = PG_GETARG_MACADDR_P(0);

    // Allocate memory for result
    macaddr *result = (macaddr *) palloc(sizeof(macaddr));

    // Keep manufacturer bytes (OUI)
    result->a = addr->a;
    result->b = addr->b;
    result->c = addr->c;

    // Zero out device-specific bytes
    result->d = 0;
    result->e = 0;
    result->f = 0;

    PG_RETURN_MACADDR_P(result);
}
```