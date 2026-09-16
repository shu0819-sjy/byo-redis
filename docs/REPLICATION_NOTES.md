# Replication notes (v0.1)

## FULLRESYNC backlog flush

During `PSYNC` full resync the master:

1. Snapshots the keyspace to RDB bytes
2. Registers the replica link in **`syncing`** state so `propagate` appends to an ordered **backlog** instead of writing the socket
3. Sends `FULLRESYNC` + RDB and drains
4. **Flush-until-empty**: under a shared `_state_lock` (also used by sync `propagate`), either take the next backlog batch or **atomically** mark the link **`live`** if empty; write the batch; drain; repeat

This closes **R3-BACKLOG-FLUSH-GAP**: writes that arrive while `await drain()` yields the event loop remain on the syncing backlog and are flushed on the next iteration, instead of being stranded after a single-shot flush followed by a premature `live` transition.

Test hooks (for regression only):

- `MasterReplication.full_resync_pause_hook` — after syncing register, before RDB write
- `MasterReplication.full_resync_flush_hook` — inside the flush loop after a batch write, before drain
