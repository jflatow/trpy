import cffi

ffic = cffi.FFI()

ffic.set_source('trpy.__trpy__', '''
   #include <traildb.h>
   #include "tdb_iter.h"
   #include "funneldb.h"
''', sources=['src/tdb_iter.c',
              'src/funneldb.c'],
     include_dirs=['src'],
     libraries=['traildb'],
     extra_compile_args=['-std=c99', '-D_DEFAULT_SOURCE'])

ffic.cdef('''
   typedef struct tdb_cons tdb_cons;
   typedef struct tdb tdb;

   typedef struct tdb_event tdb_event;
   typedef struct tdb_cursor tdb_cursor;
   typedef struct tdb_iter tdb_iter;

   typedef int tdb_error;

   typedef uint32_t tdb_field;
   typedef uint64_t tdb_val;
   typedef uint64_t tdb_item;

   struct tdb_event {
       uint64_t timestamp;
       uint64_t num_items;
       const tdb_item items[0];
       ...;
   };

   struct tdb_cursor {
       struct tdb_decode_state *state;
       const char *next_event;
       uint64_t num_events_left;
       ...;
   };

   struct tdb_iter {
       uint64_t marker;
       struct tdb_cursor *cursor;
       ...;
   };

   tdb_cons *tdb_cons_init(void);
   tdb_error tdb_cons_open(tdb_cons *cons,
                           const char *root,
                           const char **ofield_names,
                           uint64_t num_ofields);
   void tdb_cons_close(tdb_cons *cons);

   tdb_error tdb_cons_add(tdb_cons *cons,
                          const uint8_t uuid[16],
                          const uint64_t timestamp,
                          const char **values,
                          const uint64_t *value_lengths);
   tdb_error tdb_cons_append(tdb_cons *cons, const tdb *db);
   tdb_error tdb_cons_finalize(tdb_cons *cons);

   tdb *tdb_init(void);
   tdb_error tdb_open(tdb *db, const char *root);
   void tdb_close(tdb *db);

   uint64_t tdb_num_trails(const tdb *db);
   uint64_t tdb_num_events(const tdb *db);
   uint64_t tdb_num_fields(const tdb *db);
   uint64_t tdb_min_timestamp(const tdb *db);
   uint64_t tdb_max_timestamp(const tdb *db);
   uint64_t tdb_version(const tdb *db);
   const char *tdb_error_str(tdb_error errcode);

   uint64_t tdb_lexicon_size(const tdb *db, tdb_field field);

   tdb_error tdb_get_field(const tdb *db,
                           const char *field_name,
                           tdb_field *field);
   const char *tdb_get_field_name(const tdb *db, tdb_field field);
   tdb_item tdb_get_item(const tdb *db,
                         tdb_field field,
                         const char *value,
                         uint64_t value_length);
   const char *tdb_get_value(const tdb *db,
                             tdb_field field,
                             tdb_val val,
                             uint64_t *value_length);
   const char *tdb_get_item_value(const tdb *db,
                                  tdb_item item,
                                  uint64_t *value_length);

   const uint8_t *tdb_get_uuid(const tdb *db, uint64_t trail_id);
   tdb_error tdb_get_trail_id(const tdb *db,
                              const uint8_t uuid[16],
                              uint64_t *trail_id);

   struct tdb_event_filter *tdb_event_filter_new(void);
   tdb_error tdb_event_filter_add_term(struct tdb_event_filter *filter,
                                       tdb_item term,
                                       int is_negative);
   tdb_error tdb_event_filter_new_clause(struct tdb_event_filter *filter);
   void tdb_event_filter_free(struct tdb_event_filter *filter);
   tdb_error tdb_event_filter_get_item(const struct tdb_event_filter *filter,
                                       uint64_t clause_index,
                                       uint64_t item_index,
                                       tdb_item *item,
                                       int *is_negative);

   tdb_cursor *tdb_cursor_new(const struct tdb *db);
   const struct tdb_event *tdb_cursor_next(struct tdb_cursor *cursor);
   void tdb_cursor_free(struct tdb_cursor *cursor);
   tdb_error tdb_get_trail(struct tdb_cursor *cursor, uint64_t trail_id);
   uint64_t tdb_get_trail_length(struct tdb_cursor *cursor);
   tdb_error tdb_cursor_set_event_filter(struct tdb_cursor *cursor,
                                         const struct tdb_event_filter *filter);

   tdb_iter *tdb_iter_new(const tdb *db, const struct tdb_event_filter *filter);
   tdb_iter *tdb_iter_next(tdb_iter *iter);
   void tdb_iter_free(tdb_iter *iter);
   typedef void *(*tdb_fold_fn)(const tdb *, uint64_t, const tdb_event *, void *);
''')

FDB_PARAMS = 4096
FDB_EZ_MAX = 128

ffic.cdef('''
   typedef uint32_t fdb_eid;
   typedef uint32_t fdb_fid;
   typedef uint16_t fdb_mask;

   typedef struct fdb_cons fdb_cons;
   typedef struct fdb_iter fdb_iter;
   typedef struct _fdb_set fdb_set;

   typedef struct {
     fdb_eid id;
     fdb_mask mask;
   } fdb_elem;

   typedef struct {
     uint8_t flags;
     uint64_t offs;
     fdb_eid length;
   } fdb_funnel;

   typedef struct {
     unsigned int num_keys;
     fdb_fid key_offs[%d];
     tdb_field key_fields[%d];
     tdb_field mask_field;
     ...;
   } fdb_ez;

   typedef struct {
     uint64_t data_offs;
     uint64_t data_size;
     uint8_t params[%d];
     fdb_fid num_funnels;
     ...;
   } fdb;

   typedef struct {
     uint64_t terms;
     uint64_t nterms;
   } fdb_clause;

   typedef struct {
     unsigned int num_clauses;
     fdb_clause *clauses;
   } fdb_cnf;

   typedef struct _fdb_set_simple {
     fdb *db;
     fdb_fid funnel_id;
     fdb_cnf *cnf;
   } fdb_set_simple;

   typedef struct _fdb_set_complex {
     fdb *db;
     unsigned int num_sets;
     fdb_set **sets;
     fdb_cnf *cnf;
   } fdb_set_complex;

   typedef struct _fdb_family {
     fdb *db;
     unsigned int num_sets;
     fdb_fid funnel_id;
     fdb_cnf **cnfs;
   } fdb_family;

   struct _fdb_set {
     uint8_t flags;
     union {
       fdb_set_simple simple;
       fdb_set_complex complex;
     };
   };

   fdb *fdb_create(tdb *tdb, tdb_fold_fn probe, fdb_fid num_funnels, void *params);
   void fdb_detect(fdb_fid funnel_id, fdb_eid id, fdb_mask bits, fdb_cons *state);

   fdb *fdb_easy(tdb *tdb, fdb_ez *params);
   fdb *fdb_dump(fdb *db, int fd);
   fdb *fdb_load(int fd);
   fdb *fdb_free(fdb *db);

   fdb_iter *fdb_iter_new(const fdb_set *set);
   fdb_elem *fdb_iter_next(fdb_iter *iter);
   fdb_iter *fdb_iter_free(fdb_iter *iter);

   int fdb_count_set(const fdb_set *set, fdb_eid *count);
   int fdb_count_family(const fdb_family *family, fdb_eid *counts);
''' % (FDB_EZ_MAX, FDB_EZ_MAX, FDB_PARAMS))

if __name__ == '__main__':
    ffic.compile(verbose=True)
