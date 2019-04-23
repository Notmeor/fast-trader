import os
import sys

from .utils import get_win_user_documents_dir


if sys.platform == 'win32':
    doc_dir = get_win_user_documents_dir()
    os.environ['FAST_TRADER_HOME'] = os.path.join(doc_dir, 'fast_trader')
else:
    os.environ['FAST_TRADER_HOME'] = os.expanduser('~/work/fast_trader')

