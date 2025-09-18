# bbsink_throttle_manifest_contents

## Location
[src/backend/backup/basebackup_throttle.c:121-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_throttle.c#L121-L133)

## Overview
Applies throttling to manifest contents data transfer and forwards the data to the next sink in the basebackup pipeline.

## Definition


## Detailed Description
The  function handles manifest contents data transfer while applying bandwidth throttling controls. Like its archive contents counterpart, it operates in two sequential steps: first applying the throttling mechanism to control the transfer rate based on the specified data length, then forwarding the manifest contents to the next sink in the pipeline.

This function ensures that manifest data transfers are subject to the same rate limiting as other backup data, maintaining consistent bandwidth usage across all components of the basebackup process.

## Parameters / Member Variables
- : Pointer to the base bbsink structure, cast to bbsink_throttle for accessing throttling-specific functionality
- : Size of the manifest contents data being transferred (in bytes)

## Dependencies
- Functions called/Symbols referenced:
  -  (applies throttling mechanism based on data length)
  -  (forwards data to next sink in pipeline)
  -  (structure type for casting)
- Called from (representative examples):
  - Used as callback through bbsink_throttle_ops function pointer table

## Notes and Other Information
- This is a static function, used only within the basebackup_throttle.c module
- Part of the bbsink operation callback interface for handling manifest contents
- The throttling is applied before forwarding, ensuring rate limits are enforced
- Works in conjunction with the throttle() function to implement the actual rate limiting logic
- Handles specifically manifest contents data, as opposed to archive or other types of backup data
- Mirror functionality to bbsink_throttle_archive_contents but for manifest data