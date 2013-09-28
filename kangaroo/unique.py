import time
import hashlib
import random

def generate_aleatory_string():
    """Returns an unique string of 32 chars
    """
    n = random.randint(0, 10000)
    t = time.time()
    m = hashlib.md5()
    m.update((str(t) + str(n)).encode("utf-8"))
    return m.hexdigest()