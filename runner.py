import logging
import sys

from load_data import LoadData
from data import data_map

log = logging.getLogger()
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
log.addHandler(handler)

if __name__ == '__main__':

    l = LoadData(logger=log, load_data=True)
    success = l.fetch_data(data_definition=data_map, root_directory='/var/tmp/data')
    if success:
        log.info("Finished saving data")