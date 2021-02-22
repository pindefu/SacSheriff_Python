from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from arcgis.mapping import WebMap

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


def get_logger(t_dir, t_filename, s_time):

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
        os.path.join(l_dir, "Log_{}_{}_{}.txt".format(t_filename, logger_date, logger_time)), "w"
    )
    log_handler.setLevel(logging.INFO)
    log.addHandler(log_handler)

    log.info("Script Started: {} - {}".format(logger_date, logger_time))

    return log, l_dir

def createViews(task, out_folder):

    sourceItem = gis.content.get(task["itemId"])
    sourceTitle = sourceItem.title
    
    flc = FeatureLayerCollection.fromitem(sourceItem)

    for view_suffix in task["views_suffixes"]:
        view_title = sourceTitle + view_suffix

        items = gis.content.search("title:" + view_title)
        if len(items) > 0:
            logger.info(" View exists. Skipping it {}: {}".format(view_title, items[0].id))
        else:
            logger.info(" Creating view: {}".format(view_title))
            view_item = flc.manager.create_view(name=view_title, capabilities=flc.properties["capabilities"]) #'Query,Create,Update,Delete,Uploads,Editing,Sync,Extract')            
            logger.debug(" Created view {} id: {}".format(view_title, view_item.id))
            try:
                view_item.move(out_folder)
            except Exception:
                logger.info(" Unable to move view {} id: {} to folder".format(view_title, view_item.id, out_folder))


if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Get Logger & Log Directory
    logger, log_dir = get_logger(this_dir, this_filename, start_time)

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, "./config/config_createViews.json"))
    the_portal = parameters["the_portal"]
    # token = the_portal['token']

    #gis = GIS(the_portal["path"], client_id=the_portal["client_id"])
    gis = GIS("pro")

    try:
        target_folder = parameters["target_folder"]
        gis.content.create_folder(folder=target_folder)
        
        views_to_create = parameters["views_to_create"]        
        for task in views_to_create:
            if task["skip"] == False:
                logger.info("\n\n processing {}".format(task["title"]))
                createViews(task, target_folder)

    except Exception:
        logger.info("current time: {}".format(time.strftime("%H:%M:%S", time.localtime())))
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info(
            "Program Run Time: {0} Minutes".format(
                round(((time.time() - start_time) / 60), 2)
            )
        )

