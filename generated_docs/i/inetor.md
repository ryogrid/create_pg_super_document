# inetor

## Location
[src/backend/utils/adt/network.c:1914-1945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1914-L1945)

## Overview
Performs a bitwise OR operation between two inet addresses, returning the bitwise disjunction of corresponding address bits.

## Definition
```c
Datum inetor(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the bitwise OR operator (|) for PostgreSQL's inet data type. It takes two inet addresses as input and returns a new inet address where each bit position contains the result of ORing the corresponding bits from both input addresses. The function requires both addresses to be of the same family (both IPv4 or both IPv6) and will raise an error if they differ.

The operation iterates through each byte of the IP addresses and applies the bitwise OR operator (|). The resulting subnet mask is set to the maximum of the two input subnet masks, ensuring the result covers the more specific of the two subnets.

## Parameters / Member Variables
- Input parameter 0 (via PG_GETARG_INET_PP(0)): The first inet address for the OR operation
- Input parameter 1 (via PG_GETARG_INET_PP(1)): The second inet address for the OR operation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP (PostgreSQL argument retrieval macro)
  - inet (struct type for network addresses)
  - [palloc0](../p/palloc0.md) (PostgreSQL zero-initialized memory allocation)
  - ip_family (get address family)
  - ereport (PostgreSQL error reporting)
  - [errcode](../e/errcode.md)/errmsg (error handling macros)
  - ip_addrsize (get size of IP address in bytes)
  - ip_addr (get pointer to IP address bytes)
  - ip_bits (get/set subnet mask bits)
  - Max (maximum value macro)
  - SET_INET_VARSIZE (set variable-length type size)
  - PG_RETURN_INET_P (PostgreSQL return macro)
- Called from (representative examples):
  - No direct callers found (likely called through SQL operator interface)

## Notes and Other Information
- Implements the PostgreSQL | operator for inet types
- Requires both operands to be of the same address family (IPv4 or IPv6)
- Raises ERRCODE_INVALID_PARAMETER_VALUE error for mismatched address families
- [Result](../R/Result.md) subnet mask is the maximum of the two input subnet masks
- Allocates new inet structure for the result using palloc0
- Accessible from SQL as the | operator (e.g., '192.168.1.1'::inet | '0.0.0.255'::inet)
- Part of PostgreSQL's network address manipulation functions
- Useful for network address calculations and bitwise operations
- Complementary to the inetand function for complete bitwise operations

## Simplified Source

```c
Datum inetor(PG_FUNCTION_ARGS) {
    inet *address1 = PG_GETARG_INET_PP(0);
    inet *address2 = PG_GETARG_INET_PP(1);

    // Allocate memory for result
    inet *result_address = (inet *) palloc0(sizeof(inet));

    // Check that both addresses are the same family (IPv4 or IPv6)
    if (ip_family(address1) != ip_family(address2)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot OR inet values of different sizes")));
    }

    // Get address size and pointers to address bytes
    int address_size = ip_addrsize(address1);
    unsigned char *bytes1 = ip_addr(address1);
    unsigned char *bytes2 = ip_addr(address2);
    unsigned char *result_bytes = ip_addr(result_address);

    // Apply bitwise OR to each address byte
    for (int i = 0; i < address_size; i++) {
        result_bytes[i] = bytes1[i] | bytes2[i];
    }

    // Set result metadata
    ip_bits(result_address) = Max(ip_bits(address1), ip_bits(address2));
    ip_family(result_address) = ip_family(address1);
    SET_INET_VARSIZE(result_address);

    PG_RETURN_INET_P(result_address);
}
```