from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection

import traceback
import datetime
import logging
import time
import json
import os
import sys
import os.path
from os import path
import copy


def get_config(in_file):

    with open(in_file) as config:
        param_dict = json.load(config)

    return param_dict


def get_logger(t_dir, s_time):

    log = logging.getLogger(__name__)
    log.setLevel(1)

    # Set Logger Time
    logger_date = datetime.datetime.fromtimestamp(s_time).strftime("%Y_%m_%d")
    logger_time = datetime.datetime.fromtimestamp(s_time).strftime("%H_%M_%S")

    # Debug Handler for Console Checks - logger.info(msg)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    log.addHandler(console_handler)

    # Ensure Logs Directory Exists
    l_dir = os.path.join(t_dir, "logs", logger_date)
    if not os.path.exists(l_dir):
        os.makedirs(l_dir)

    # Log Handler for Reports - logger.info(msg)
    log_handler = logging.FileHandler(
        os.path.join(l_dir, "GP_Report_{}_{}.txt".format(logger_date, logger_time)), "w"
    )
    log_handler.setLevel(logging.INFO)
    log.addHandler(log_handler)

    log.info("Script Started: {} - {}".format(logger_date, logger_time))

    return log, l_dir

if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]

    # Get Logger & Log Directory
    logger, log_dir = get_logger(this_dir, start_time)

    gis = GIS("pro")
    me = gis.users.me
    try:

        folders = ["Park Evaluation", "Park Evaluation Demo", "Search and Rescue"]

        for folder in folders:
            folder_items = me.items(folder=folder)

            for item in folder_items:
                item.protect(enable=False)
                logger.info("Deleting {}".format(item.title))
                try:
                    item.delete()
                except Exception:
                    logger.info("-- Couldn't delete {}".format(item.title))

    
    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info(
            "Program Run Time: {0} Minutes".format(
                round(((time.time() - start_time) / 60), 2)
            )
        )

