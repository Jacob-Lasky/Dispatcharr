This adapter installs the core reservation guard in existing Dispatcharr 0.34.1
web workers through the plugin loader, without restarting an active stream.
`apps/m3u/reservation_guard.py` is the implementation; copy that file alongside
this directory's `plugin.py`, `plugin.json` and `__init__.py` into the persistent
plugin directory `m3u_pool_guard`, then enable it and reload plugins once.
A reload installs the guard only in a worker that has none: a worker that
already runs a guard reuses it, code included. A new guard version therefore
reaches a web worker only when that worker process starts again; restart the
container so every worker runs the same version.

On Tower this is `/mnt/cache/appdata/dispatcharr/data/plugins/m3u_pool_guard/`
(also visible through `/mnt/user/appdata/dispatcharr/data/`). The existing
dockerMan template `my-Dispatcharr.xml` binds its parent to `/data`; both the
plugin files and the enabled setting in `/data/db` survive image updates and
container recreation. No image-layer core file, fork image, startup script or
additional mount is used. The loader installs the guard in each web worker
at startup. This is a runtime adapter for the core fix, not a fork image.

The guard checks the `__version__` stamped in the image's `version.py`
and the SHA-256 of all ten hooked core/loader source files before installing or re-enabling the guard. Only
Dispatcharr 0.34.1 with the source contract from upstream revision
`83f82bf388b72ec1e6cc356f1015a9812228b424` is admitted. An upstream version or
source mismatch logs `m3u_slot guard refused incompatible core` and refuses
installation, leaving the updated upstream implementation intact. Do not
add new supported hashes without validating the lifecycle against that build.
The image labels say 0.34.1 although that revision's source `version.py` says
0.34.0; the version check uses the image's stamped version.

The manifest is version 2 (Dispatcharr 0.34's sandboxed plugin contract) and
declares one capability, `proxy_internals`, the disclosure for hooking the live
proxy's slot accounting. Dispatcharr asks an admin to approve it when the
plugin is enabled. The guard deliberately does not use 0.34's leader-elected
service (`on_leader_acquired`): it is not a background service but a set of
hooks every web worker that serves streams must carry, so it installs in each
worker from `Plugin.__init__`, as before. Installing creates no thread, writes
no files and needs no plugin data directory.

0.2.2 changed only the manifest, the Status output and the version string, so
its guard logic is the same as 0.2.1's. Deploying it over a running 0.2.1 needs a plugin reload,
not a restart. Status reports both `version` (the adapter) and
`guard_version` (the guard each worker actually runs); they differ until that
worker next starts.

Dispatcharr 0.34 runs every plugin action on a Celery worker, which never
installs the guard, so Status there reads the guard's own registration
instead: `web_workers` lists each live uWSGI worker on this host that
installed it, with the guard version it runs, and `installed` means at least
one did. `reclamation_enabled`, `grace_seconds` and `source_file` live in each
web worker's memory, so a Celery-served Status reports them as null.

The plugin's Status action reports compatibility, loaded source path,
`guard_version`, `reclamation_enabled` and `grace_seconds`. Startup/reload logs include
`guard active ... reclamation_enabled=True grace_seconds=180`; reservation
events separately include owner, client ID, profile and generation token.

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
Profile movement gives back or carries a credential slot only while the
owner's old profile counter is above zero, the same rule as release. A stock
VOD release that was never paired (JAKE-670) can leave that counter at zero;
a later move then takes a fresh credential slot instead of a sibling's.
Ranked Matchups 1.38.5 supplies a task owner tied to its global probe lock.

Dispatcharr 0.34 runs plugin discovery on its `plugin-leader-election`
thread as well as at app start, so each worker can construct the plugin twice
at once. Under uWSGI's gevent patching that thread can start as a greenlet on
the same hub. `install()` serializes on a process-wide native lock, which a
waiter polls while yielding to gevent, and publishes the guard only after
every hook is in place; a failed install restores the hooks.
Maintenance reclaims only while this worker's loader holds the plugin enabled,
so disabling it stops reclamation in every worker after their rediscovery,
not only in the worker that served the request. The check runs once per
maintenance pass, so a pass that began before a disable finishes its batch.
Reclaim follows the plugin entry this worker's loader published last. When
two discovery scans straddle a disable or re-enable, the older scan can
publish last, and that worker keeps the older state until a later discovery
publishes.

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
