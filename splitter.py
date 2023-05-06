"""
Modeule provides methods for splitting users
"""
from hashlib import sha512


def splitter(userid, n_groups: int = 2, hash_func: callable = sha512, salt: str = "exp_name"):
    return userid % n_groups
