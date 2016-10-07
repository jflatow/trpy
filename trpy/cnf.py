from operator import __and__, __or__

def noneOf(*args, **kwds):
    return Q([Clause([Literal((k, v), True) for k, vs in kwds.items() for v in vs] +
                     [Literal(a, True) for a in args])])

def oneOf(*args, **kwds):
    return Q([Clause([Literal((k, v)) for k, vs in kwds.items() for v in vs] +
                     [Literal(a) for a in args])])

def where(*args, **kwds):
    return Q([Clause([Literal(i)]) for i in kwds.items()] +
             [Clause([Literal(a)]) for a in args])

def whereNot(*args, **kwds):
    return Q([Clause([Literal(i, True)]) for i in kwds.items()] +
             [Clause([Literal(a, True)]) for a in args])

class Q(object):
    def __init__(self, clauses):
        self.clauses = frozenset(c for c in clauses if c.literals)

    def __and__(self, other):
        return Q(self.clauses | other.clauses)

    def __or__(self, other):
        if not self.clauses:
            return other
        if not other.clauses:
            return self
        return Q(c | d for c in self.clauses for d in other.clauses)

    def __invert__(self):
        if not self.clauses:
            return self
        return Q.wrap(reduce(__or__, (~c for c in self.clauses)))

    def __cmp__(self, other):
        return cmp(str(self), str(other))

    def __eq__(self, other):
        return self.clauses == other.clauses

    def __hash__(self):
        return hash(self.clauses)

    def __str__(self):
        format = '(%s)' if len(self.clauses) > 1 else '%s'
        return ' & '.join(format % c for c in self.clauses)

    def replace(self, fun):
        for clause in self.clauses:
            for literal in clause.literals:
                literal.term = fun(literal.term)
        return self

    @classmethod
    def parse(cls, string, ext=False):
        import re
        if isinstance(string, cls):
            return string
        if ext:
            string = string.replace('!', '~').replace(',', '&').replace('+', '|')
        return eval(re.sub(r'([^&|~()\s][^&|~()]*)',
                           r'Q.wrap("""\1""".strip())', string) or 'Q([])')

    @classmethod
    def wrap(cls, proposition):
        if isinstance(proposition, Q):
            return proposition
        if isinstance(proposition, Clause):
            return Q((proposition, ))
        if isinstance(proposition, Literal):
            return Q((Clause((proposition, )), ))
        return Q((Clause((Literal(proposition), )), ))

class Clause(object):
    def __init__(self, literals):
        self.literals = frozenset(literals)

    def __and__(self, other):
        return Q.wrap(self) & Q.wrap(other)

    def __or__(self, other):
        return Clause(self.literals | other.literals)

    def __invert__(self):
        return Q(Clause([~l]) for l in self.literals)

    def __eq__(self, other):
        return self.literals == other.literals

    def __hash__(self):
        return hash(self.literals)

    def __str__(self):
        return ' | '.join('%s' % l for l in self.literals)

class Literal(object):
    def __init__(self, term, negated=False):
        self.term = term
        self.negated = negated

    def __and__(self, other):
        return Clause((self, )) & Clause((other, ))

    def __or__(self, other):
        return Clause((self, other))

    def __invert__(self):
        return type(self)(self.term, negated=not self.negated)

    def __eq__(self, other):
        return self.term == other.term and self.negated == other.negated

    def __hash__(self):
        return hash(self.term) ^ hash(self.negated)

    def __str__(self):
        if isinstance(self.term, tuple) and len(self.term) == 2:
            return '%s%s%s' % (self.term[0], '!=' if self.negated else '=', self.term[1])
        return '%s%s' % ('!' if self.negated else '', self.term)
