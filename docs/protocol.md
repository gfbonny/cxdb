# Binary Protocol (v1)

All messages are sent over a persistent TCP connection with length-prefixed frames.

## Frame Header

```
FrameHeader {
  len: u32      // payload length
  msg_type: u16
  flags: u16
  req_id: u64
}
```

All fields are little-endian.

## Message Types

| msg_type | name        |
|---------:|-------------|
| 1        | HELLO       |
| 2        | CTX_CREATE  |
| 3        | CTX_FORK    |
| 4        | GET_HEAD    |
| 5        | APPEND_TURN |
| 6        | GET_LAST    |
| 9        | GET_BLOB    |
| 10       | ATTACH_FS   |
| 11       | PUT_BLOB    |
| 255      | ERROR       |

## Requests / Responses

### CTX_CREATE

Request payload:
```
base_turn_id: u64   // 0 for empty context
```

Response payload:
```
context_id: u64
head_turn_id: u64
head_depth: u32
```

### CTX_FORK

Request payload:
```
base_turn_id: u64
```

Response payload (same as CTX_CREATE).

### GET_HEAD

Request payload:
```
context_id: u64
```

Response payload (same as CTX_CREATE).

### APPEND_TURN

Request payload:
```
context_id: u64
parent_turn_id: u64        // 0 means use current head

declared_type_id_len: u32
declared_type_id: [bytes]
declared_type_version: u32

encoding: u32              // 1 = msgpack
compression: u32           // 0 = none, 1 = zstd
uncompressed_len: u32
content_hash_b3_256: [32]

payload_len: u32
payload_bytes: [payload_len]

idempotency_key_len: u32
idempotency_key: [bytes]

// If frame flags bit 0 is set, append:
fs_root_hash: [32]
```

Response payload:
```
context_id: u64
new_turn_id: u64
new_depth: u32
content_hash_b3_256: [32]
```

### GET_LAST

Request payload:
```
context_id: u64
limit: u32
include_payload: u32  // 0 or 1
```

Response payload:
```
count: u32
items[count]:
  turn_id: u64
  parent_turn_id: u64
  depth: u32
  declared_type_id_len: u32
  declared_type_id: [bytes]
  declared_type_version: u32
  encoding: u32
  compression: u32
  uncompressed_len: u32
  content_hash_b3_256: [32]
  payload_len: u32          // only when include_payload=1
  payload_bytes: [payload_len]
```

When `include_payload=1`, payloads are returned uncompressed (compression=0).

### GET_BLOB

Request payload:
```
content_hash_b3_256: [32]
```

Response payload:
```
raw_len: u32
raw_bytes: [raw_len]
```

### ATTACH_FS

Request payload:
```
turn_id: u64
fs_root_hash: [32]
```

Response payload:
```
turn_id: u64
fs_root_hash: [32]
```

### PUT_BLOB

Request payload:
```
content_hash_b3_256: [32]
raw_len: u32
raw_bytes: [raw_len]
```

Response payload:
```
content_hash_b3_256: [32]
was_new: u8   // 1=new, 0=already stored
```

### ERROR

Response payload:
```
code: u32
detail_len: u32
detail_bytes: [detail_len]
```

Common codes: 404, 422, 500.
