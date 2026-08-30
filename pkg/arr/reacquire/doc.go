// Package reacquire replaces media a Decypharr entry can no longer serve.
//
// It binds each managed file to the Sonarr or Radarr library file that points
// at it, so a confirmed content failure can be turned into an Arr action on
// exact database IDs rather than a name or size guess. A binding comes from
// one of three exact sources: the symlink a library file resolves to, the
// stream identity a Decypharr .strm file carries, or the download history the
// Arr recorded on import. Only those authorize a destructive action.
//
// Indexer keeps the bindings current: a full reconciliation per Arr on start
// and on demand, and a targeted pass for one entry after a download completes.
// Service persists them, and turns a Reacquire request into an idempotent job
// that deletes the Arr file, fails the exact grab history so the Arr
// blocklists the release, searches for a replacement, and reports ready once
// the replacement is imported. Every Arr mutation is recorded as an intent
// before it is sent and confirmed against the Arr afterwards, so a retry after
// a crash or an unclear response never blocklists or searches twice.
//
// Only Sonarr and Radarr are supported; other Arr kinds have no equivalent
// file, history, and search APIs, and are left alone.
package reacquire
