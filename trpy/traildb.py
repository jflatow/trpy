from .__trpy__ import ffi, lib

from collections import namedtuple
from datetime import datetime
from time import mktime

def item_is32(item): return not (item & 128)
def item_field32(item): return item & 127
def item_val32(item): return (item >> 8) & 4294967295L # UINT32_MAX

def item_field(item):
    if item_is32(item):
        return item_field32(item)
    else:
        return (item & 127) | (((item >> 8) & 127) << 7)

def item_val(item):
    if item_is32(item):
        return item_val32(item)
    else:
        return (item >> 16)

def hex_cookie(cookie):
    if isinstance(cookie, basestring):
        return cookie
    return ffi.buffer(cookie, 16)[:].encode('hex')

def raw_cookie(cookie):
    if isinstance(cookie, basestring):
        return ffi.cast('uint8_t *', ffi.from_buffer(cookie.decode('hex')))
    return cookie

def events(cursor, evify):
    while True:
        ev = lib.tdb_cursor_next(cursor)
        if not ev:
            break
        yield evify(ev.timestamp, (ev.items[i] for i in xrange(ev.num_items)))

class TDBError(Exception):
    def __init__(self, msg, err=None):
        self.msg = msg
        self.err = err

    def __str__(self):
        if self.err is None:
            return self.msg
        return '%s (%s)' % (self.msg, ffi.string(lib.tdb_error_str(self.err)))

class TDBCons(object):
    def __init__(self, path, ofields=()):
        a = [ffi.new('char []', f) for f in ofields]
        b = ffi.new('char *[]', a)
        c = self._cons = lib.tdb_cons_init()
        e = lib.tdb_cons_open(c, path, b, len(b))
        if e:
            raise TDBError("Failed to open constructor", e)

        self.path = path
        self.ofields = ofields

    def __del__(self):
        if hasattr(self, '_cons'):
            lib.tdb_cons_close(self._cons)

    def add(self, cookie, timestamp, values=()):
        if isinstance(timestamp, datetime):
            timestamp = int(mktime(timestamp.timetuple()))
        a = [ffi.new('char []', v) for v in values]
        b = ffi.new('char *[]', a)
        l = ffi.new('uint64_t []', [len(v) for v in values])
        f = lib.tdb_cons_add(self._cons, raw_cookie(cookie), timestamp, b, l)
        if f:
            raise TDBError("Too many values: %s" % values[f])

    def append(self, db):
        f = lib.tdb_cons_append(self._cons, db._db)
        if f < 0:
            raise TDBError("Wrong number of fields: %d" % db.num_fields)
        if f > 0:
            raise TDBError("Too many values: %s" % values[f])

    def finalize(self):
        e = lib.tdb_cons_finalize(self._cons)
        if e:
            raise TDBError("Could not finalize (%d)" % e)
        return TDB(self.path)

class TDB(object):
    def __init__(self, path):
        d = self._db = lib.tdb_init()
        e = lib.tdb_open(d, path)
        if e:
            raise TDBError("Could not open %s" % path, e)

        self.num_trails = lib.tdb_num_trails(d)
        self.num_events = lib.tdb_num_events(d)
        self.num_fields = lib.tdb_num_fields(d)
        self.fields = [ffi.string(lib.tdb_get_field_name(d, i)) for i in xrange(self.num_fields)]

        self._evcls = namedtuple('event', self.fields, rename=True)
        self._ui64p = ffi.new('uint64_t []', 1)

    def __del__(self):
        if hasattr(self, '_db'):
            lib.tdb_close(self._db)

    def __contains__(self, cookieish):
        try:
            self[cookieish]
            return True
        except IndexError:
            return False

    def __getitem__(self, cookieish):
        if isinstance(cookieish, basestring):
            return self.trail(self.cookie_id(cookieish))
        return self.trail(cookieish)

    def __iter__(self):
        return iter(self.match())

    def __len__(self):
        return self.num_trails

    def evifier(self, expand=False):
        if expand:
            return lambda t, items: self._evcls(t, *(self.item_value(i) for i in items))
        else:
            return lambda t, items: self._evcls(t, *(item_val(i) for i in items))

    def match(self, query=None, **kwds):
        return TDBIter(self, query=query, **kwds)

    def trail(self, id, **kwds):
        return TDBCursor(self, **kwds).at(id).events()

    def field(self, fieldish):
        if isinstance(fieldish, basestring):
            try:
                return self.fields.index(fieldish)
            except ValueError:
                raise KeyError("No such field: '%s'" % fieldish)
        return fieldish

    def field_name(self, fieldish):
        if isinstance(fieldish, basestring):
            return fieldish
        try:
            return self.fields[fieldish]
        except IndexError:
            raise IndexError("No such field: '%s'" % fieldish)

    def lexicon(self, fieldish):
        field = self.field(fieldish)
        return [self.lexicon_word(field, i) for i in xrange(1, self.lexicon_size(field))]

    def lexicon_size(self, fieldish):
        field = self.field(fieldish)
        value = lib.tdb_lexicon_size(self._db, field)
        return value

    def lexicon_word(self, fieldish, val):
        field = self.field(fieldish)
        value = lib.tdb_get_value(self._db, field, val, self._ui64p)
        if value is None:
            raise IndexError("Field '%s' has no such value: '%s'", (self.field_name(fieldish), val))
        return ffi.string(value, self._ui64p[0])

    def lexicon_val(self, fieldish, value):
        return item_val(self.item(fieldish, value))

    def item(self, fieldish, value):
        field = self.field(fieldish)
        item = lib.tdb_get_item(self._db, field, value, len(value))
        if not item:
            raise KeyError("Field '%s' has no such value: '%s'" % (self.field_name(fieldish), value))
        return item

    def item_value(self, item):
        value = lib.tdb_get_item_value(self._db, item, self._ui64p)
        if value is None:
            raise ValueError("Bad item")
        return ffi.string(value, self._ui64p[0])

    def cookie(self, id):
        cookie = lib.tdb_get_uuid(self._db, id)
        if cookie:
            return hex_cookie(cookie)
        raise IndexError("Cookie id out of range")

    def cookie_id(self, cookie):
        if lib.tdb_get_trail_id(self._db, raw_cookie(cookie), self._ui64p):
            raise IndexError("UUID '%s' not found" % cookie)
        return self._ui64p[0]

    def time_range(self):
        tmin = lib.tdb_min_timestamp(self._db)
        tmax = lib.tdb_max_timestamp(self._db)
        return tmin, tmax

class TDBIter(object):
    def __init__(self, db, query=None, **kwds):
        self.filter = TDBFilter(db, query) if query else None
        self._iter = lib.tdb_iter_new(db._db, self.filter._filter if query else ffi.NULL)
        self.evify = db.evifier(**kwds)

    def __del__(self):
        if hasattr(self, '_iter'):
            lib.tdb_iter_free(self._iter)

    def __iter__(self):
        return self

    def next(self):
        i = lib.tdb_iter_next(self._iter)
        if not i:
            raise StopIteration()
        return i.marker - 1, list(events(i.cursor, self.evify))

class TDBCursor(object):
    def __init__(self, db, **kwds):
        self._cursor = lib.tdb_cursor_new(db._db)
        self.evify = db.evifier(**kwds)

    def __del__(self):
        if hasattr(self, '_cursor'):
            lib.tdb_cursor_free(self._cursor)

    def at(self, trail_id):
        e = lib.tdb_get_trail(self._cursor, trail_id)
        if e:
            raise TDBError("Failed to get trail", e)
        return self

    def events(self):
        return list(events(self._cursor, self.evify))

class TDBFilter(object):
    def __init__(self, db, query):
        self._filter = lib.tdb_event_filter_new()
        for i, clause in enumerate(query.clauses):
            if i > 0:
                e = lib.tdb_event_filter_new_clause(self._filter)
                if e:
                    raise TDBError("Failed to create clause", e)
            for literal in clause.literals:
                fieldish, value = literal.term
                field = db.field(fieldish)
                item = lib.tdb_get_item(db._db, field, value, len(value))
                e = lib.tdb_event_filter_add_term(self._filter, item, literal.negated)
                if e:
                    raise TDBError("Failed to add term", e)

    def __del__(self):
        if hasattr(self, '_filter'):
            lib.tdb_event_filter_free(self._filter)
