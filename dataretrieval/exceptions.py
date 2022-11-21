class DataRetrievalError(BaseException):
    """Base exception for dataretrieval"""


class EmptyQueryResultError(DataRetrievalError):
    """Raised when a query returns an error"""
