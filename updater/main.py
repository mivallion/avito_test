import time
from datetime import datetime

from common import *

while True:
    ts = int(timestamp())
    if ts % 3600 == 0:
        update_queries()
        update_top_ads()
