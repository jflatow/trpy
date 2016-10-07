from .__trpy__ import ffi, lib
from .cnf import Q, oneOf, noneOf, where, whereNot

from itertools import product
from urllib import quote_plus, unquote_plus

FDB_DENSE   = 1
FDB_SPARSE  = 2

FDB_SIMPLE  = 1
FDB_COMPLEX = 2

def keymap(num_keys, key_fields):
    combo = []
    for i, f in enumerate(key_fields):
        if not num_keys:
            return
        if f:
            combo.append(f)
        else:
            num_keys -= 1
            yield tuple(sorted(combo)), i - len(combo)
            combo = []

def seqify(x):
    if isinstance(x, basestring):
        return x,
    try:
        x[0]
        return x
    except IndexError:
        return x
    except KeyError, TypeError:
        return x,

class FDBError(Exception):
    pass

class FDB(object):
    def __init__(self, tdb, _db):
        self._db = _db
        self.tdb = tdb
        self.num_funnels = _db.num_funnels
        self.params = ffi.cast('fdb_ez *', _db.params)
        self.keymap = dict(keymap(self.params[0].num_keys, self.params[0].key_fields))
        self.masked = tdb.fields[self.params[0].mask_field]

    def __del__(self):
        lib.fdb_free(self._db)

    def __len__(self):
        return self.num_funnels

    def __getitem__(self, id):
        return self.funnel(id)

    def funnel(self, id, cnf=None):
        if id < 0 or id >= len(self):
            raise IndexError("Invalid funnel id: %s" % id)
        return FDBSimpleSet(self, id, cnf)

    def funnel_id(self, key):
        if isinstance(key, basestring):
            key = dict(map(unquote_plus, i.split('=')) for i in key.split(',') if i)
        if isinstance(key, dict):
            tdb = self.tdb
            O, F = self.params[0].key_offs, self.params[0].key_fields
            which = dict((tdb.field(k), v) for k, v in key.items())
            index = self.keymap[tuple(sorted(which))]
            id = O[index - 1] if index else 0
            for i, f in enumerate(F[index:len(F)]):
                if not f:
                    break
                id += tdb.lexicon_val(f, which[f]) * O[index + i]
            return id
        return key

    def keys(self, serialize=False):
        tdb = self.tdb
        for _, key in sorted((v, k) for k, v in self.keymap.items()):
            names = [quote_plus(tdb.fields[f]) for f in key]
            for which in product(*(map(quote_plus, tdb.lexicon(f)) for f in key)):
                if serialize:
                    yield ','.join('%s=%s' % item for item in zip(names, which))
                else:
                    yield dict(zip(names, which))

    def mask_val(self, value):
        field = self.params[0].mask_field
        return self.tdb.lexicon_val(field, value)

    def any(self, *sets):
        """
        Create a disjunction which avoids the 64 term limitation for a CNF.
        """
        if len(sets) > 64:
            sets = sets[:63] + (self.any(*sets[63:]), )
        return FDBComplexSet(self, sets, FDBCNF(oneOf(*range(len(sets)))))

    def all(self, *sets):
        """
        Create a conjunction which avoids the 64 term limitation for a CNF.
        """
        if len(sets) > 64:
            sets = sets[:63] + (self.any(*sets[63:]), )
        return FDBComplexSet(self, sets, FDBCNF(where(*range(len(sets)))))

    def select(self, **kwds):
        """
        Select a single row (funnel) that can be used to build up complex sets.
        """
        cnf = FDBCNF(kwds.pop(self.masked, Q([])), self.mask_val)
        return self.funnel(self.funnel_id(kwds), cnf)

    def query(self, query):
        """
        Create a complex set from a query string.
        """
        return FDBSet.parse(self, query)

    def family(self, queries):
        """
        Create a family from a list of mask query strings.
        """
        return FDBFamily.parse(self, queries)

    @classmethod
    def make(cls, tdb, keys=((),), mask_field=1):
        if mask_field:
            mask_arity = tdb.lexicon_size(mask_field)
            if mask_arity < 1:
                raise ValueError("Mask field has no values")
            if mask_arity > 8:
                raise ValueError("Mask field cardinality is too high")
        params = ffi.new('fdb_ez []', 1)
        params[0].num_keys = len(keys)
        params[0].mask_field = mask_field or 0
        i = 0
        for group in keys:
            for k in seqify(group):
                params[0].key_fields[i] = tdb.field(k)
                i += 1
            i += 1
        return cls(tdb, lib.fdb_easy(tdb._db, params))

    @classmethod
    def load(cls, tdb, file):
        db = lib.fdb_load(file.fileno())
        if not db:
            raise FDBError("Could not open %s" % path)
        return cls(tdb, db)

    def dump(self, file):
        lib.fdb_dump(self._db, file.fileno())

class FDBIter(object):
    def __init__(self, set):
        self._iter = lib.fdb_iter_new(set._set)
        self.set = set

    def __del__(self):
        lib.fdb_iter_free(self._iter)

    def __iter__(self):
        return self

    def next(self):
        n = lib.fdb_iter_next(self._iter)
        if not n:
            raise StopIteration()
        return n.id, n.mask

class FDBCNF(object):
    def __init__(self, q, mapping=lambda t: t):
        self._cnf = ffi.new('fdb_cnf []', 1)
        self._cnf[0].num_clauses = len(q.clauses)
        self._cnf[0].clauses = self._clauses = ffi.new('fdb_clause []', len(q.clauses))
        for n, clause in enumerate(q.clauses):
            for literal in clause.literals:
                index = mapping(literal.term)
                if index > 64:
                    raise ValueError("Term index is too large: %s", index)
                if literal.negated:
                    self._clauses[n].nterms |= 1 << index
                else:
                    self._clauses[n].terms |= 1 << index

class FDBSet(object):
    def __and__(self, other):
        return FDBComplexSet(self.db, [self, other], FDBCNF(where(0, 1)))

    def __or__(self, other):
        return FDBComplexSet(self.db, [self, other], FDBCNF(oneOf(0, 1)))

    def __xor__(self, other):
        return FDBComplexSet(self.db, [self, other], FDBCNF((where(0) & whereNot(1)) | where(1) & whereNot(0)))

    def __mul__(self, other):
        return FDBComplexSet(self.db, [self, other], FDBCNF(where(0, 1)))

    def __add__(self, other):
        return FDBComplexSet(self.db, [self, other], FDBCNF(oneOf(0, 1)))

    def __sub__(self, other):
        return FDBComplexSet(self.db, [self, other], FDBCNF(where(0) & whereNot(1)))

    def __iter__(self):
        return FDBIter(self)

    def __len__(self):
        count = ffi.new('fdb_eid []', 1)
        lib.fdb_count_set(self._set, count)
        return count[0]

    def trails(self, **kwds):
        for id, mask in self:
            yield id, self.db.tdb.trail(id, **kwds)

    @classmethod
    def parse(cls, db, string):
        return FDBComplexSet.parse(db, string)

class FDBSimpleSet(FDBSet):
    def __init__(self, db, funnel_id, cnf=None):
        self._set = ffi.new('fdb_set []', 1)
        self._set[0].flags = FDB_SIMPLE
        self._set[0].simple.db = db._db
        self._set[0].simple.funnel_id = funnel_id
        self._set[0].simple.cnf = cnf._cnf if cnf else ffi.NULL
        self.db = db
        self.funnel_id = funnel_id
        self.cnf = cnf

    @classmethod
    def parse(cls, db, string):
        key, mask = string.split('/') if '/' in string else (string, '')
        return cls(db, db.funnel_id(key), FDBCNF(Q.parse(mask, ext=True), db.mask_val))

class FDBComplexSet(FDBSet):
    def __init__(self, db, sets, cnf):
        self._set = ffi.new('fdb_set []', 1)
        self._set[0].flags = FDB_COMPLEX
        self._set[0].complex.db = db._db
        self._set[0].complex.num_sets = len(sets)
        self._set[0].complex.sets = self._sets = ffi.new('fdb_set *[]', [s._set for s in sets])
        self._set[0].complex.cnf = cnf._cnf
        self.db = db
        self.sets = sets
        self.cnf = cnf

    @classmethod
    def parse(cls, db, string):
        q = Q.parse(string).replace(lambda t: FDBSimpleSet.parse(db, t))
        sets = list(set(l.term for c in q.clauses for l in c.literals))
        return cls(db, sets, FDBCNF(q, sets.index))

class FDBFamily(object):
    def __init__(self, db, cnfs):
        self._family = ffi.new('fdb_family []', 1)
        self._family[0].db = db._db
        self._family[0].num_sets = len(cnfs)
        self._family[0].cnfs = self._cnfs = ffi.new('fdb_cnf *[]', [c._cnf for c in cnfs])
        self._counts = ffi.new('fdb_eid []', len(cnfs))
        self.db = db
        self.cnfs = cnfs

    def counts(self, key):
        self._family[0].funnel_id = self.db.funnel_id(key)
        lib.fdb_count_family(self._family, self._counts)
        return tuple(self._counts)

    @classmethod
    def parse(cls, db, strings):
        return cls(db, [FDBCNF(Q.parse(s, ext=True), db.mask_val) for s in strings])