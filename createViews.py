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
import inspect


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


def class_to_json(obj):
    #logger.info("-----------entering class_to_json {}".format(obj))
    newJson = {}

    for i in inspect.getmembers(obj):

        # to remove private and protected functions
        if not i[0].startswith('_'):

            # To remove other methods that
            # doesnot start with a underscore
            if not inspect.ismethod(i[1]):
                newJson[i[0]] = i[1]
    return newJson


def ensure_folder(target_folder):
    me = gis.users.me
    my_folders = (me.folders)
    folder_list = [i['title'] for i in my_folders]
    if target_folder not in folder_list:
        gis.content.create_folder(target_folder)
        logger.info("   Created target folder: {}".format(target_folder))


def createViews(task, views_created):

    sourceItem = gis.content.get(task["itemId"])
    sourceTitle = sourceItem.title

    flc_source = FeatureLayerCollection.fromitem(sourceItem)

    for view_to_create in task["views_to_create"]:
        view_suffix = view_to_create["view_suffix"]

        view_title = sourceTitle + view_suffix

        logger.info("\n------- View to create: {}".format(view_title))

        target_folder = view_to_create["target_folder"]

        ensure_folder(target_folder)

        items = gis.content.search("title:" + view_title)
        if len(items) > 0:
            logger.info(" View exists. Delete it: {} -- {}".format(view_title, items[0].id))
            items[0].delete()
            #view_item = items[0]
            #flc = FeatureLayerCollection.fromitem(view_item)

        logger.info(" Creating view: {}".format(view_title))

        view_item = flc_source.manager.create_view(name=view_title)
        views_created.append({view_title: view_item.id})
        logger.info(" Created view {} id: {}".format(view_title, view_item.id))

        try:
            view_item.move(target_folder)
        except Exception:
            logger.info(" Unable to move view {} to folder".format(view_title, target_folder))

        # update the item level definition: capabilities and editor tracking
        flc = FeatureLayerCollection.fromitem(view_item)
        editableLayerDefinition = view_to_create["editableLayerDefinition"]
        flc.manager.update_definition(editableLayerDefinition)

        # update the layer level definition: filter and default values
        if "sub_definitions" in view_to_create:
            sub_definitions = view_to_create["sub_definitions"]

            for sub_definitions in sub_definitions:
                target = None
                # find the sub layer or table
                if "layerIdInItem" in sub_definitions:
                    target = view_item.layers[sub_definitions["layerIdInItem"]]
                if "tableIdInItem" in sub_definitions:
                    target = view_item.tables[sub_definitions["tableIdInItem"]]

                # update filter
                if "viewDefinitionQuery" in sub_definitions:
                    viewDefinitionQuery = sub_definitions["viewDefinitionQuery"]
                    logger.info("\n  ---- applying view definitin query {}".format(viewDefinitionQuery))
                    target.manager.update_definition({"viewDefinitionQuery": viewDefinitionQuery})

                # default values are in the templates.
                # process the templates class member variables
                if "default_values" in sub_definitions:
                    default_values = sub_definitions["default_values"]
                    newTemplates, templates_type = replace_defaults_in_template_json(default_values, target)

                    # update layer or table templates with default values
                    if len(newTemplates) > 0:
                        if templates_type == "templates":
                            logger.info("\n  ---- applying templates with default values ----")
                            target.manager.update_definition({"templates": newTemplates})
                        elif templates_type == "types.templates":
                            logger.info("\n  ---- applying  types.templates with default values ----")
                            target.manager.update_definition({"types": newTemplates})

        # Share the view
        share_settings = view_to_create["share_settings"]
        view_item.share(everyone=share_settings["everyone"], org=share_settings["org"], groups=share_settings["groups"])


def replace_defaults_in_template_json(default_values, target):

    logger.info("   default values setting: {}".format(default_values))

    # get the existing templates of the layer or table
    target_properties = target.manager.properties
    target_type = target_properties.type
    logger.info("   target type: {}".format(target_type))

    templates = target_properties.templates
    templates_type = ""
    if templates is None or len(templates) == 0:
        # if target_properties.types and target_properties.types.templates:
        templates_type = "types.templates"
        # needs to loop through the templates of each type
        #templates = target_properties.types.templates
    else:
        templates_type = "templates"

    if templates is None or len(templates) == 0:
        logger.info("   No templates found")
        return [], templates_type

    newTemplates = []
    for template in templates:
        # get the template json
        templateJSON = class_to_json(template)

        prototype = class_to_json(templateJSON["prototype"])
        attributes = class_to_json(prototype["attributes"])

        # replace the default values
        for k, v in attributes.items():
            if k in default_values:
                attributes[k] = default_values[k]

        prototype.pop("attributes", None)

        prototype["attributes"] = attributes
        prototype["_constructed"] = True

        templateJSON.pop("prototype", None)
        templateJSON["prototype"] = prototype

        logger.info("  New template JSON with default values{}".format(templateJSON))
        newTemplates.append(templateJSON)

    return newTemplates, templates_type


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
    # the_portal = parameters["the_portal"]
    # token = the_portal['token']

    # gis = GIS(the_portal["path"], client_id=the_portal["client_id"])
    gis = GIS("pro")

    views_created = []
    try:
        tasks = parameters["tasks"]
        for task in tasks:
            if task["skip"] == False:
                logger.info("\n\n ***************** processing {} ***************** \n".format(task["title"]))
                createViews(task, views_created)

        logger.info("\n\nCreated {} views: {}".format(len(views_created), views_created))

    except Exception:
        logger.info("current time: {}".format(time.strftime("%H:%M:%S", time.localtime())))
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info("\n\nProgram Run Time: {0} Minutes".format(round(((time.time() - start_time) / 60), 2)))
