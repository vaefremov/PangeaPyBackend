# $Id:  $
"""
Exceptions that can be raised within p4db and p4db_pg modules.
"""
__version__ = "$Revision: 0 $"[11:-2]  # code version

class DBException(RuntimeError):
    "Exception for P4 DB operations"
    def __init__(self, a_cause):
        self.cause = a_cause

    def __str__(self):
        return self.cause

    def __repr__(self):
        return str(self)

class DBNotFoundException(DBException):
    "Logical corruption in database"
    def __init__(self, a_cause):
        DBException.__init__(self, a_cause)

class DBCorruptionException(DBException):
    "Logical corruption in database"
    def __init__(self, a_cause):
        DBException.__init__(self, a_cause)

class DBAuthoritiesException(DBException):
    "Wrong authorities"
    def __init__(self, a_cause):
        DBException.__init__(self, a_cause)

