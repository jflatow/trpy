#include "tdb_iter.h"

tdb_iter *tdb_iter_new(const tdb *db, const struct tdb_event_filter *filter) {
  tdb_iter *iter = calloc(1, sizeof(tdb_iter));
  if (iter == NULL)
    return NULL;
  if ((iter->cursor = tdb_cursor_new(db)) == NULL) {
    free(iter);
    return NULL;
  }
  if (filter)
    if (tdb_cursor_set_event_filter(iter->cursor, filter) != TDB_ERR_OK) {
      tdb_iter_free(iter);
      return NULL;
    }
  return iter;
}

tdb_iter *tdb_iter_next(tdb_iter *restrict iter) {
  while (iter->marker++ < tdb_num_trails(iter->cursor->state->db)) {
    if (tdb_get_trail(iter->cursor, iter->marker - 1) == TDB_ERR_OK)
      if (tdb_cursor_peek(iter->cursor))
        return iter;
  }
  return NULL;
}

void tdb_iter_free(tdb_iter *iter) {
  free(iter->cursor);
  free(iter);
}

void *tdb_fold(const tdb *db, const struct tdb_event_filter *filter, tdb_fold_fn fun, void *acc) {
  tdb_iter *iter = tdb_iter_new(db, filter);
  const tdb_event *event;
  while (tdb_iter_next(iter))
    while ((event = tdb_cursor_next(iter->cursor)))
      acc = fun(db, iter->marker - 1, event, acc);
  tdb_iter_free(iter);
  return acc;
}
