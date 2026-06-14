// Package pgque installs PgQue for Orange's optional mapped-split scheduler.
//
// Setup responsibilities are intentionally narrow:
//   - install the upstream PgQue SQL into the target database,
//   - create the Orange mapped-split build queue,
//   - register the logical consumer, and
//   - optionally register a cooperative subconsumer identity.
//
// Setup must not run Orange store migrations and must not create Orange store
// tables. The reverse is also true: Orange store migrations must not install
// PgQue. Keeping these tracks independent lets embedders use the durable
// Postgres store without the async scheduler, or install PgQue before the
// store exists.
//
// Multiple replicas may call Setup during process startup. Setup uses a
// transaction-scoped advisory lock around the PgQue SQL install and queue
// wiring so concurrent callers for the same database and queue serialize.
// PgQue's SQL also creates cluster-level roles such as pgque_reader and
// pgque_writer. Because roles are cluster-global while advisory locks are
// database-local, the vendored SQL catches duplicate role creation races; this
// matters when tests or operators install PgQue into multiple databases in the
// same Postgres cluster at the same time.
//
// Queue names, consumer names, and subconsumer names are limited to PgQue-safe
// portable identifiers. PgQue uses queue names in LISTEN/NOTIFY channels, so
// Setup enforces the same 57-byte queue-name limit as the PgQue SQL.
//
// Tests and manual smoke checks should remember that sending an event only
// stores it in PgQue's event table. A consumer sees events after PgQue
// materializes a tick window. ReceiveOne is a test helper that forces the next
// tick and runs the global ticker before receiving; production workers should
// normally rely on their scheduler loop rather than calling ReceiveOne.
package pgque
