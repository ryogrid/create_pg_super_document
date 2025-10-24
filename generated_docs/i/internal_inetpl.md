# internal_inetpl

## Location
[src/backend/utils/adt/network.c:1946-1997](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1946-L1997)

## Overview
A static helper function that performs addition of a signed 64-bit integer value to an inet address, handling IP address arithmetic with proper overflow detection and carry propagation.

## Definition
static inet *internal_inetpl(inet *ip, int64 addend)

## Detailed Description
The `internal_inetpl` function implements IP address arithmetic by adding a 64-bit signed integer to an inet address structure. It performs byte-by-byte addition starting from the least significant byte, propagating carries through the address bytes. The function carefully handles both positive and negative addends, ensuring proper arithmetic overflow detection.

The implementation includes sophisticated overflow checking - after processing all bytes, it verifies that the final state has either zero addend and carry (for positive original addend) or -1 addend and carry 1 (for negative original addend). Any other combination indicates arithmetic overflow and triggers an error.

The function preserves the original address family and netmask bits while creating a new inet structure with the computed result.

## Parameters / Member Variables
- `ip`: Input inet address structure to which the addend will be added
- `addend`: Signed 64-bit integer value to add to the IP address

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - ip_addrsize
  - ip_addr
  - ip_bits
  - ip_family
  - SET_INET_VARSIZE
  - ereport
- Called from (representative examples):
  - [inetpl](inetpl.md)
  - [inetmi_int8](inetmi_int8.md)

## Notes and Other Information
- Uses careful bit manipulation to avoid platform-specific right-shift behavior on negative numbers
- Implements robust overflow detection for both positive and negative arithmetic
- Preserves the original inet structure's family and netmask while computing new address
- Memory allocation uses palloc0 to ensure proper PostgreSQL memory management
- Part of PostgreSQL's network data type arithmetic operations infrastructure

## Simplified Source

```c
static inet *internal_inetpl(inet *input_address, int64 addend) {
    // Allocate memory for result
    inet *result_address = (inet *) palloc0(sizeof(inet));

    // Get address size and pointers to address bytes
    int address_size = ip_addrsize(input_address);
    unsigned char *input_bytes = ip_addr(input_address);
    unsigned char *result_bytes = ip_addr(result_address);
    int carry = 0;

    // Process bytes from least significant to most significant
    for (int byte_index = address_size - 1; byte_index >= 0; byte_index--) {
        // Add current byte + addend's low byte + carry from previous byte
        carry = input_bytes[byte_index] + (int)(addend & 0xFF) + carry;
        result_bytes[byte_index] = (unsigned char)(carry & 0xFF);
        carry >>= 8;

        // Shift addend right by one byte (avoiding negative right-shift issues)
        addend &= ~((int64) 0xFF);  // Clear low byte
        addend /= 0x100;            // Divide by 256 instead of right-shift
    }

    // Check for arithmetic overflow
    // Valid end states: (addend=0, carry=0) or (addend=-1, carry=1)
    bool valid_positive_result = (addend == 0 && carry == 0);
    bool valid_negative_result = (addend == -1 && carry == 1);

    if (!valid_positive_result && !valid_negative_result) {
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("result is out of range")));
    }

    // Copy metadata from input to result
    ip_bits(result_address) = ip_bits(input_address);
    ip_family(result_address) = ip_family(input_address);
    SET_INET_VARSIZE(result_address);

    return result_address;
}
```