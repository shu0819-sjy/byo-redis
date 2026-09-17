# Replication notes (v0.2.2)

## FULLRESYNC backlog flush

During `PSYNC` full resync the master:

1. Acquires the same async write barrier used by all mutating commands
2. Registers the replica link in **`syncing`** state and snapshots the keyspace while writes are excluded
3. Releases the barrier; later writes append to a byte-bounded ordered **backlog**
4. Encodes and sends `FULLRESYNC` + checksummed RDB outside the event-loop thread
5. **Flush-until-empty**: under a shared `_state_lock`, either take the next backlog batch or **atomically** mark the link **`live`** if empty; write the batch; drain; repeat

The write barrier prevents both snapshot/backlog duplication and the pre-registration lost-write window. Flush-until-empty closes **R3-BACKLOG-FLUSH-GAP**: writes that arrive while `await drain()` yields remain on the syncing backlog and are flushed on the next iteration.

Test hooks (for regression only):

- `MasterReplication.full_resync_pause_hook` — after syncing register, before RDB write
- `MasterReplication.full_resync_flush_hook` — inside the flush loop after a batch write, before drain
