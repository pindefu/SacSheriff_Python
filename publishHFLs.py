import arcpy
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

def publishSD(in_sd, pLyrConfig, service_name, editableLayerDefinition):

    logger.info("Uploading Service Definition...")
    result = arcpy.UploadServiceDefinition_server(in_sd, 'My Hosted Services')
    logger.info("Successfully Uploaded service.")
    search_result = gis.content.search(query="title:"+service_name, item_type="Feature Layer")
    item_published = search_result[0]
    logger.info("Item published {}".format(item_published))

    if pLyrConfig["enable_editing"]:
        flc = FeatureLayerCollection.fromitem(item_published)
        flc.manager.update_definition(editableLayerDefinition)
        logger.info("Successfully updated services definition to make the layer editable.")

    #item_published.protect(enable=True)
    #logger.info("Enaled delete protection")

    return item_published


if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Get Logger & Log Directory
    logger, log_dir = get_logger(this_dir, this_filename, start_time)

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, "./config/config_publishHFLs.json"))
    the_portal = parameters["the_portal"]
    # token = the_portal['token']

    #gis = GIS(the_portal["path"], client_id=the_portal["client_id"])
    gis = GIS("pro")

    try:
        aprx = arcpy.mp.ArcGISProject(parameters["arcgis_pro_project"])
        m = aprx.listMaps(parameters["map_name_in_prj"])[0]
        path_to_store_service_definition_files = parameters[
            "path_to_store_service_definition_files"
        ]
        
        service_name_prefix = parameters["service_name_prefix"]
        service_name_suffix = parameters["service_name_suffix"]
        target_folder_in_portal = parameters["target_folder_in_portal"]

        editableLayerDefinition = parameters["editableLayerDefinition"]

        layers_to_publish = parameters["layers_to_publish"]
        seconds_to_sleep_between_layers = parameters["seconds_to_sleep_between_layers"]
        result_layers = {}
        failed_layers = []
        
        all_layers_in_map = []
        for l in m.listLayers():
            all_layers_in_map.append(l.name)

        all_tables_in_map = []
        for t in m.listTables():
            all_tables_in_map.append(t.name)

        for idx, pLyrConfig in enumerate(layers_to_publish):
            if pLyrConfig["skip"] == False:
                if idx > 0:
                    time.sleep(seconds_to_sleep_between_layers)
                    
                try: 
                    lyr_start_time = time.time()
                    names_in_pro_map = pLyrConfig["names_in_pro_map"]
                    logger.info("\n\n----------- layer to publish: {} -----------------".format(names_in_pro_map))

                    lyrs = []
                    for name in names_in_pro_map:
                        if (name in all_layers_in_map):
                            lyrs.append(m.listLayers(name)[0])
                        elif (name in all_tables_in_map):
                            lyrs.append(m.listTables(name)[0])
                        else:
                            raise Exception("Unable to find the layer or table {}".format(name))

                    if len(lyrs)==0:
                        logger.error(" __ERROR____{}\n\n".format("Not such layer(s) found in the ArcGIS Pro project"))
                        continue 

                    if len(lyrs)==1:
                        lyrs=lyrs[0]

                    logger.info("lyrs {}".format(lyrs))

                    service_name = service_name_prefix + pLyrConfig["service_name"] + service_name_suffix
                    overwrite_existing_sd = pLyrConfig["overwrite_existing_sd"]
                    overwrite_existing_service = pLyrConfig["overwrite_existing_service"]

                    service_available = gis.content.is_service_name_available(service_name=service_name, service_type = 'featureService')
                    if not service_available:
                        logger.warning(" Service name exists: {}".format(service_name))
                        if not overwrite_existing_service:
                            existing_item = gis.content.search(query="title:"+service_name, item_type="Feature Layer")[0]                        
                            result_layers[pLyrConfig["service_name"]] = existing_item.id
                            continue
                    
                    out_sd = path_to_store_service_definition_files + "\\" + service_name + ".sd"                    
                    if path.exists(out_sd) or path.exists(out_sd + "draft"):
                        if overwrite_existing_sd:
                            if path.exists(out_sd):
                                os.remove(out_sd)
                                logger.info("Existing SD file removed")
                            if path.exists(out_sd + "draft"):
                                os.remove(out_sd + "draft")
                                logger.info("Existing SD draft file removed")
                        
                    if not path.exists(out_sd):
                        if not path.exists(out_sd + "draft"):
                            sddraft = arcpy.mp.CreateWebLayerSDDraft(
                                lyrs,
                                out_sd + "draft",
                                service_name,
                                pLyrConfig["server_type"],
                                pLyrConfig["service_type"],
                                folder_name=target_folder_in_portal,
                                overwrite_existing_service = overwrite_existing_service,
                                copy_data_to_server=True,
                                enable_editing=pLyrConfig["enable_editing"],
                                allow_exporting=pLyrConfig["allow_exporting"],
                                enable_sync=pLyrConfig["enable_sync"],
                                summary=pLyrConfig["summary"],
                                tags=pLyrConfig["tags"],
                                description=pLyrConfig["description"])
                            logger.info("SD Draft file {} generated ".format(out_sd + "draft"))


                        arcpy.StageService_server(out_sd + "draft", out_sd)
                        logger.info("SD file {} generated ".format(out_sd))

                    item_published = publishSD(out_sd, pLyrConfig, service_name, editableLayerDefinition)                    
                    # Generate the sourceItemIds parameter for use with createWebMaps.py:
                    result_layers[pLyrConfig["service_name"]] = item_published.id
                    # In case there are multiple layers in the service, add the look up of these layers and the item id to the result
                    if len(names_in_pro_map) > 1:
                        for name in names_in_pro_map:
                            if name != pLyrConfig["service_name"]:
                                result_layers[name] = item_published.id                    

                except Exception:
                    for name in names_in_pro_map:
                        failed_layers.append(name)
                    logger.info("current time: {}".format(time.strftime("%H:%M:%S", time.localtime())))               
                    logger.info(traceback.format_exc())
                finally:
                    lyr_end_time = time.time()
                    logger.info(" ... Layer run time: {0} Minutes".format(round(((time.time() - lyr_start_time) / 60), 2)))                    
                    

        logger.info("Result layers: {}".format(result_layers))
        if len(failed_layers):
            logger.info("Failed to publish these layers: {}".format(failed_layers))

    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info(
            "Program Run Time: {0} Minutes".format(
                round(((time.time() - start_time) / 60), 2)
            )
        )

