#ifndef __TDB_ITER_H__
#define __TDB_ITER_H__

#include <stdint.h>
#include <traildb.h>

typedef struct {
  uint64_t marker;
  tdb_cursor *cursor;
} tdb_iter;

tdb_iter *tdb_iter_new(const tdb *db, const struct tdb_event_filter *filter);
tdb_iter *tdb_iter_next(tdb_iter *iter);
void tdb_iter_free(tdb_iter *iter);

typedef void *(*tdb_fold_fn)(const tdb *, uint64_t, const tdb_event *, void *);
void *tdb_fold(const tdb *db, const struct tdb_event_filter *filter, tdb_fold_fn fun, void *acc);

struct tdb_decode_state {
  const tdb *db;

  /* internal buffer */
  void *events_buffer;
  uint64_t events_buffer_len;

  /* trail state */
  uint64_t trail_id;
  const char *data;
  uint64_t size;
  uint64_t offset;
  uint64_t tstamp;

  /* options */
  const tdb_item *filter;
  uint64_t filter_len;
  uint64_t filter_size;

  int edge_encoded;
  tdb_item previous_items[0];
};

#endif /* __TDB_ITER_H__ */
