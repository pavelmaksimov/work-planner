from typing import Literal


class Statuses:
    add = "ADD"
    run = "RUN"
    success = "SUCCESS"
    error = "ERROR"
    fatal_error = "FATAL_ERROR"
    error_statuses = (error, fatal_error)
    run_statuses = (run,)
    LiteralT = Literal[add, run, success, error, fatal_error]


class Error:
    expired = "ExpiredError"


class Operators:
    equal = "="
    not_equal = "!="
    less = "<"
    more = ">"
    more_or_equal = ">="
    less_or_equal = "<="

    like = "like"
    not_like = "not_like"
    ilike = "ilike"
    not_ilike = "not_ilike"
    in_ = "in"
    not_in = "not_in"
    contains = "contains"
    not_contains = "not_contains"

    LiteralT = Literal[
        less,
        equal,
        more,
        not_equal,
        more_or_equal,
        less_or_equal,
        like,
        not_like,
        ilike,
        not_ilike,
        in_,
        not_in,
        contains,
        not_contains,
    ]
