This adapter installs the core reservation guard in existing Dispatcharr 0.33
web workers through the plugin loader, without restarting an active stream.
`apps/m3u/reservation_guard.py` is the implementation; copy that file alongside
this directory's `plugin.py`, `plugin.json` and `__init__.py` into the persistent
plugin directory `m3u_pool_guard`, then enable it and reload plugins once.

The W466 defect is a cross-worker reconnect race. TS output can release its
assignment before `remove_client()` publishes the disconnect marker. Two
workers in `ChannelService.cancel_pending_shutdown()` can then both call
`Channel.get_stream()` before either publishes an assignment, reserving two
slots but publishing one metadata release token. The assignment mutex covers
lookup, reservation and publication across workers, including preview workers.

Live and preview slots have atomic Redis owner records. Acquisition, release,
profile movement and abandoned-owner reclamation update the matching counters
with Lua. Logs include profile, owner, client ID, worker and generation token;
provider URLs and credentials are never included. Existing streams without an
owner record keep the original release path. VOD and catch-up retain their
session managers' existing reservation pairing and are logged as unbound.
Ranked Matchups 1.38.5 supplies a task owner tied to its global probe lock.

The existing proxy maintenance loop reclaims only reservations older than
180 seconds whose assignment mutex, proxy metadata, owner key, client set
and individual client hashes are all absent. Probe reservations require their
global task lock to be absent. It never stops clients or resets a profile's
whole counter. Release tombstones and generation-bound client cleanup prevent late cleanup
from consuming a replacement reservation. The installed context survives plugin
reloads; maintenance rotates its bounded scan so live owners cannot starve an
abandoned owner. Legacy anonymous counters cannot safely be inferred
and are never automatically reset.

Run the regression suite only against a disposable Redis database:

```
RESERVATION_GUARD_TEST_REDIS=redis://127.0.0.1:6379/15 \
  python manage.py test apps.m3u.tests.test_reservation_guard
```

The tests flush that explicitly named database. `W466_BROKEN=1` reproduces the
original double reservation in the concurrency witness. Normal test discovery
skips this integration class unless the isolated Redis URL is supplied.
