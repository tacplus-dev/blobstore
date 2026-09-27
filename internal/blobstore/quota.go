package blobstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrQuotaExceeded is the refusal that means the tenant is full. It is a sentinel
// so CreateUpload can tell it apart from "the record store could not be read",
// which also refuses the upload but is a different answer to the caller.
var ErrQuotaExceeded = errors.New("blobstore: volume group quota exceeded")

// Quota enforces the per-tenant, per-group ceiling (R4). There is no global
// tenant quota: quotas hang off the group, so the unit of accounting is the pair
// (group, namespace) and nothing else.
//
// USAGE IS READ FROM THE RECORD STORE, not from a counter this service keeps.
// The sum of a tenant's non-deleted Blob rows in a group IS the usage, and it is
// the only number that survives a pod restart, a rolling upgrade, or a pod that
// was never told about a blob the other pod committed. Keeping a counter beside
// it would introduce a second answer that can be wrong.
//
// RESERVATIONS ARE PER-POD AND IN MEMORY. CreateUpload reserves the declared size
// so that N uploads started at once cannot each see the same free space; the
// reservation is released when the session settles, and start-up has none because
// a pod that restarted has no in-flight uploads. This is the one place the design
// admits imprecision: two CreateUpload calls landing on DIFFERENT pods at the same
// moment each see the other's reservation as absent, so a tenant can overshoot its
// quota by at most the total bytes of the uploads in flight on one pod. Closing
// that needs a shared reservation ledger — a stratum read-modify-write per upload,
// which is the cost the whole design is avoiding — and the overshoot is bounded,
// temporary and self-correcting as sessions settle. Stated here rather than left to
// be discovered; see workers/blobstore-result.md.
type Quota struct {
	records Records

	mu           sync.Mutex
	reservations map[string]map[string]int64 // (group,namespace) key -> upload id -> bytes
}

func NewQuota(records Records) *Quota {
	return &Quota{records: records, reservations: map[string]map[string]int64{}}
}

func quotaKey(group, namespace string) string { return group + "\x00" + namespace }

// Reserve admits one upload against the tenant's quota for the group, or refuses
// it. On success the bytes are held until Release or Commit.
//
// Commit and Release do the same thing to the reservation — both drop it. They
// are separate names because what they mean differs: on commit the bytes became
// real usage and the next read of the record store will see them; on release they
// never existed. Collapsing them into one method would make that distinction
// impossible to read at the call sites.
func (q *Quota) Reserve(ctx context.Context, uploadID, group, namespace string, size int64, limit int64) error {
	used, err := q.usage(ctx, group, namespace)
	if err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	key := quotaKey(group, namespace)
	var reserved int64
	for _, n := range q.reservations[key] {
		reserved += n
	}
	if used+reserved+size > limit {
		return fmt.Errorf("%w: volume group %q quota for %s is %d bytes; %d used, %d reserved, %d requested",
			ErrQuotaExceeded, group, namespace, limit, used, reserved, size)
	}
	if q.reservations[key] == nil {
		q.reservations[key] = map[string]int64{}
	}
	q.reservations[key][uploadID] = size
	return nil
}

// Commit drops a reservation whose bytes are now promoted and recorded.
func (q *Quota) Commit(uploadID, group, namespace string) { q.drop(uploadID, group, namespace) }

// Release drops a reservation whose upload failed, was cancelled, or expired.
func (q *Quota) Release(uploadID, group, namespace string) { q.drop(uploadID, group, namespace) }

func (q *Quota) drop(uploadID, group, namespace string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := quotaKey(group, namespace)
	delete(q.reservations[key], uploadID)
	if len(q.reservations[key]) == 0 {
		delete(q.reservations, key)
	}
}

// usage sums the tenant's promoted blobs in the group.
//
// IT IS NOT CACHED, deliberately. A cache would make the quota answer stale for
// its TTL in BOTH directions — admitting an upload a tenant has no room for, and
// refusing one after a delete freed space — and a stale quota decision is harder
// to reason about than a slow one. The read is a filtered query on two indexed,
// EQ-filterable fields and happens once per CreateUpload, not once per byte.
func (q *Quota) usage(ctx context.Context, group, namespace string) (int64, error) {
	blobs, err := q.records.ListBlobsInGroup(ctx, group, namespace)
	if err != nil {
		return 0, fmt.Errorf("reading quota usage for %s/%s: %w", group, namespace, err)
	}
	var total int64
	for _, b := range blobs {
		if b.GetDeleted() {
			continue
		}
		total += b.GetSizeBytes()
	}
	return total, nil
}
