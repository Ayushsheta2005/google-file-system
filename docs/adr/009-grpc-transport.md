# ADR-009 — Raise the gRPC message cap; share channel options

**Status:** implemented · **Code:** `gfs_config.py` (`GRPC_SERVER_OPTIONS`, `GRPC_CHANNEL_OPTIONS`)

## The problem

gRPC defaults to a **4 MB** maximum message size. A GFS chunk is **64 MB**.
Any write that actually filled a chunk would be rejected by the transport, and
the error surfaces as an opaque `RESOURCE_EXHAUSTED` far from the cause.

Separately, the ten `grpc.insecure_channel(...)` sites across the three
modules had three different option sets between them — one had hand-tuned
keepalive, the rest had nothing — so behaviour depended on which code path
happened to create the channel.

## What the guidance says

[gRPC's performance guide](https://grpc.io/docs/guides/performance/) and
[Microsoft's](https://learn.microsoft.com/en-us/aspnet/core/grpc/performance)
both say to reuse channels rather than create per-call, and to chunk large
payloads into **16–64 KiB** streamed frames rather than one enormous unary
message.

Worth knowing specifically for Python: streaming RPCs there spin up extra
threads per call, and the Python implementation can be *slower* streaming than
unary for the same bytes. So the usual "always stream large payloads" advice
is not automatically right here — it has to be measured.

## Decision

- `max_send_message_length` / `max_receive_message_length` = **128 MB** on
  both servers and all channels: one 64 MB chunk plus protobuf framing plus
  headroom.
- One shared `GRPC_CHANNEL_OPTIONS` / `GRPC_SERVER_OPTIONS` applied uniformly
  at all ten channel sites and both servers, with keepalive so that a dead
  peer is detected by the transport rather than by an RPC hanging.
- **Transport framing is left as unary for now, and A/B tested in Phase 3.5**
  (`GFS_TRANSPORT=unary|stream`) rather than switched on faith.

### Rejected

- **Switching to streaming immediately.** The advice is sound in general and
  likely wrong for Python specifically. Measuring costs one benchmark; being
  wrong costs a regression nobody notices.
- **Raising the cap to 2 GB (effectively unlimited).** A bad or hostile length
  prefix then becomes a 2 GB allocation. 128 MB bounds the damage while
  comfortably fitting the largest legitimate message.

## Consequences

- A single malformed request can still make the server allocate up to 128 MB.
  Acceptable for a cluster with no untrusted clients; would not be in a
  product.
- Keepalive adds background pings on idle channels.
