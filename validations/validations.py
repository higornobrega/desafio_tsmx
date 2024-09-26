from typing import str


def is_null(value: str) -> bool:

    if value == None:
        return True
    return False


def is_empyt(value: str) -> bool:
    if value == '':
        return True
    return False
