# show_tcp_keepalives_idle

## Location
src/backend/libpq/pqcomm.c: 1971 - 1983

## Overview
A GUC (Grand Unified Configuration) show hook function that displays the current TCP keepalive idle timeout value for the current connection.

## Definition


## Detailed Description
This function serves as the display hook for the PostgreSQL GUC parameter . It retrieves the current TCP keepalive idle timeout setting for the active client connection (MyProcPort) and formats it as a string for display. The function uses a static buffer to store the formatted integer value, which represents the number of seconds before keepalive probes are sent on an idle connection.

The function is part of PostgreSQL's configuration parameter system and is called when the user queries the current value of the tcp_keepalives_idle parameter (e.g., via ).

## Parameters / Member Variables
This function takes no parameters and operates on the global MyProcPort variable representing the current client connection.

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves the actual keepalive idle timeout value for the given port
  - : Formats the integer value into a string
  - : Global variable representing the current client connection port
- Called from (representative examples):
  - GUC system when displaying parameter values

## Notes and Other Information
- Uses a static 16-character buffer to store the formatted result
- The actual keepalive logic is platform-dependent and handled by 
- Returns "0" when keepalives are not supported or not configured
- Part of PostgreSQL's libpq communication subsystem for managing client connections