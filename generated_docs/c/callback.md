# callback

## Location
src/tools/ifaddrs/test_ifaddrs.c: 46 - 55

## Overview
A static callback function used by the ifaddrs testing tool to display network interface address and netmask information in a formatted manner.

## Definition
```c
static void callback(struct sockaddr *addr, struct sockaddr *mask, void *unused)
```

## Detailed Description
The `callback` function serves as a callback handler for network interface enumeration operations. It receives address and netmask information for each network interface and formats them for display. The function prints both the address and netmask on the same line, separated by appropriate labels, making it easy to read network interface configuration information.

This function is designed to be passed as a callback parameter to functions that iterate over network interfaces, following the common callback pattern used in PostgreSQL's network interface handling code.

## Parameters / Member Variables
- `addr`: A pointer to a socket address structure containing the network interface address to be displayed
- `mask`: A pointer to a socket address structure containing the netmask for the interface
- `unused`: A void pointer parameter that is not used by this callback implementation, but is part of the callback signature for compatibility

## Dependencies
- Functions called/Symbols referenced:
  - `print_addr` (called twice: at line 49 for address, at line 51 for mask)
  - `printf` (for formatting output)
- Called from (representative examples):
  - Extensively used throughout PostgreSQL codebase as a callback function pattern
  - Various bulk delete operations (brinbulkdelete, ginbulkdelete, gistbulkdelete, etc.)
  - Network interface enumeration functions
  - Vacuum and cleanup operations
  - Transaction callback systems

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (test_ifaddrs.c)
- The function follows a standard callback pattern where it receives structured data and processes/displays it
- The output format is: "addr: [address]  mask: [netmask]" followed by a newline
- The `unused` parameter suggests this callback signature is standardized for compatibility with various callback systems
- Despite being a simple test utility function, the `callback` symbol name appears extensively throughout the PostgreSQL codebase, indicating this is a common naming pattern for callback functions
- The function is designed for testing network interface address enumeration functionality