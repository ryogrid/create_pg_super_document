# PQEnvironmentOption

## Location
src/interfaces/libpq/libpq-int.h: 268 - 272

## Overview
PQEnvironmentOption is a structure type that defines mappings between PostgreSQL environment variables and their corresponding server configuration parameters, used during client connection initialization.

## Definition


## Detailed Description
The PQEnvironmentOption structure serves as a mapping template for libpq's automatic environment variable processing during connection establishment. It pairs environment variable names (such as PGDATESTYLE, PGTZ) with their corresponding PostgreSQL server configuration parameter names (like datestyle, timezone). This structure is primarily used in the EnvironmentOptions[] array in fe-connect.c to systematically process environment variables and convert them into appropriate SET commands sent to the server during connection startup.

## Parameters / Member Variables
- `envName`: Pointer to a string containing the name of an environment variable (e.g., "PGDATESTYLE", "PGTZ")
- `pgName`: Pointer to a string containing the corresponding PostgreSQL configuration parameter name (e.g., "datestyle", "timezone")

## Dependencies
- Functions called/Symbols referenced: None (simple data structure)
- Used by:
  - internalPQconninfoOption (in fe-connect.c:372)
  - pqBuildStartupPacket3 (in fe-protocol3.c:2238)
  - build_startup_packet (in fe-protocol3.c:2261, 2264)

## Notes and Other Information
- The structure is defined in libpq-int.h as part of libpq's internal interface
- Used in the static EnvironmentOptions[] array to define environment-to-parameter mappings
- The array is null-terminated with {NULL, NULL} entry
- Environment variables mapped include common user settings (PGDATESTYLE, PGTZ) and performance settings (PGGEQO)
- This mechanism allows users to set PostgreSQL session parameters via environment variables automatically