"""
File to hold API configurations
"""

import os
_basedir = os.path.abspath(os.path.dirname(__file__))

DEBUG_MODE = True

DATABASE_CONN_INFO = {
    'host': "localhost",
    'db': "newsapp",
    'user': "user",
    'pass': "",
    'charset': 'utf8'
}
