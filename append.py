from arcgis.gis import GIS
from arcgis import features
from arcgis.features import FeatureLayerCollection
from arcgis.geometry import Geometry
from arcgis.geometry.filters import intersects
import pandas as pd

from arcgis.features import GeoAccessor, GeoSeriesAccessor
from copy import deepcopy

import traceback
import datetime
import logging
import time
import json
import os
import sys
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

def alter_tracking(item, tracking_state):
    if item == None:
        return
    logger.info("\n\n{} Editor tracking on {}\n".format(tracking_state, item.title))
    flc = FeatureLayerCollection.fromitem(item)
    cap = flc.properties["editorTrackingInfo"]
    logger.info("\n ... existng editor tracking property {}\n".format(cap))

    if tracking_state == "Disable":
        cap["enableEditorTracking"] = False

    else:
        cap["enableEditorTracking"] = True

    alter_response = ""
    try: 
        alter_response = flc.manager.update_definition({"editorTrackingInfo": cap})
    except Exception:
        logger.info("Exception {}".format(traceback.format_exc()))
    finally:        
        logger.info("Change tracking result: {}\n\n".format(alter_response))

def run_update(the_func):

    def wrapper(*args, **kwargs):

        # Run Function & Collect Update List
        edit_list = the_func(*args)

        if edit_list:

            operation = kwargs.get('operation', None)
            # Batch Update List Into Smaller Sets
            batch_size = kwargs.get('batch', None)
            use_global_ids = kwargs.get('use_global_ids', False)
            if not batch_size:
                batch_size = 1000
            update_sets = [edit_list[x:x + batch_size] for x in range(0, len(edit_list), batch_size)]
            logger.info('\nProcessing {} Batches\n'.format(len(update_sets)))

            if operation == "update" and kwargs.get('track') is not None:
                try:
                    alter_tracking(kwargs.get('track'), 'Disable')
                except RuntimeError:
                    logger.info('Alter Tracking - RunTime Error. Passing Until Testing Proves Otherwise . . .\n\n')
                    pass

            # Push Edit Batches
            try:
                for update_set in update_sets:
                    try:
                        if operation == "update":
                            edit_result = kwargs.get('update').edit_features(updates=update_set, use_global_ids=use_global_ids, rollback_on_failure = False)
                        else: # add
                            edit_result = kwargs.get('update').edit_features(adds=update_set, use_global_ids=use_global_ids, rollback_on_failure = False)
                        summarizeEditResult(operation, edit_result, update_set, kwargs.get('update'), kwargs.get('track'), kwargs.get('batch'), use_global_ids)

                    except Exception:
                        logger.info(traceback.format_exc())
            except Exception:
                logger.info(traceback.format_exc())
            finally:
                if operation == "update":
                    try:
                        alter_tracking(kwargs.get('track'), 'Enable')
                    except RuntimeError:
                        logger.info('Alter Tracking - RunTime Error. Passing Until Testing Proves Otherwise . . .')
                        pass

        else:
            logger.info('Returned List Was Empty. No Edits Performed.')

    return wrapper

def summarizeEditResult(operation, edit_result,  update_set, update_layer, track, batch, use_global_ids):
    if operation == "update":
        keyString = "updateResults"
    else:
        keyString = "addResults"

    #logger.info('\nFeature set to update: {}\n'.format(update_set))
    #logger.info('\nEdit Results: {}\n'.format(edit_result))
    if keyString in edit_result and len(edit_result[keyString])>0:
        totalRecords = len(update_set) 
        succeeded_records = list(filter(lambda d: d["success"] == True, edit_result[keyString]))

        #failed_records = list(filter(lambda d: d["success"] == False, edit_result[keyString]))
        logger.info('\nEdit Results: {} of {} succeeded'.format(len(succeeded_records), totalRecords))
        #if len(succeeded_records)==0:
            #logger.info('\nFailed records: {}'.format(failed_records))
            # if operation == "add", then update
            #update_failed_records(failed_records, update_set, update_layer, track, batch, use_global_ids)
    else:
        logger.info('\nEdit Results: {}'.format(edit_result))


def update_failed_records(failed_records, update_set, update_layer, track, batch, use_global_ids):
    failed_set = list(filter(lambda d: d.attributes["globalid"] in failed_records["globalid"], update_set))
    update_records(failed_set, update=update_layer, track=track, operation="update", batch=batch, use_global_ids=use_global_ids)    

@run_update
def update_records(failed_set):
    return failed_set

def processTask(task, batchSize):

    dataType = task["dataType"]
    target = task["target"]
    bCheck4Existing = target["check4Existing"]

    targetLyrOrTbl=None
    sdf_to_append = None
    
    targetItem = gis.content.get(target["itemId"])
    if task["dataType"] == "LAYERS":
        targetLyrOrTbl = targetItem.layers[target["orderInItem"]]
        sdf_to_append = pd.DataFrame.spatial.from_featureclass(task["source"]) # .query("globalid == '{C28F3958-ED03-4328-B238-1A6DD5C44DA1}'")    
    else:
        targetLyrOrTbl = targetItem.tables[target["orderInItem"]]
        sdf_to_append = pd.DataFrame.spatial.from_table(task["source"], fields="*", skip_nulls=False) # .query("globalid == '{C28F3958-ED03-4328-B238-1A6DD5C44DA1}'")    

    if target["truncateFirst"] == True:
        logger.info("Deleting all records")
        targetLyrOrTbl.delete_features(where="objectid > 0")

    # query to get the schema
    schema_resp = targetLyrOrTbl.query(where="1=1", return_geometry=False, return_all_records = True, result_record_count=0)
    logger.info("schema_resp {}".format(schema_resp))
    fields_2_skip = ["creationdate", "creator", "editdate", "editor", "created_user", "created_date", "last_edited_user", "last_edited_date","SHAPE__Area", "SHAPE__Length"]
    fields_2_skip.append("objectid")
    globalIdFieldName = "globalid"

    records_to_add = []
    globalids_to_add = []    

    for index, row in sdf_to_append.iterrows():
        if index % batchSize == 0:
            logger.info("\n \t index {}".format(index))
        #logger.info("\n \t row {}".format(row))
        
        globalids_to_add.append(row[globalIdFieldName])

        new_attributes = {}
        for f in schema_resp.fields:
            if f["name"] not in fields_2_skip:
                #logger.info("\t\t field: {}".format(f))
                val = row[f["name"]]
                if val != None and not pd.isna(val) and val != "" and val != '<Null>' and val != '<null>':
                    if f["type"] == "esriFieldTypeDate":
                        # convert pandas.tslib.Timestamp to datetime 
                        #logger.info(" to_pydatetime {}".format(row[f["name"]]))
                        new_attributes[f["name"]] = val.to_pydatetime()
                    elif f["type"] == "esriFieldTypeString":
                        val = val.replace("<Null>", "")
                        if not (val.strip() == "" or val.strip().lower()=='<null>'):
                            new_attributes[f["name"]] = val
                    # elif f["type"] == "esriFieldTypeDouble":                        
                    #     if isinstance(val, float):                         
                    #         new_attributes[f["name"]] = val
                    #     else:
                    #         logger.info("\n ------------ Str to number?{}".format(val))
                    else:                    
                        new_attributes[f["name"]] = val

        if task["dataType"] == "LAYERS":
            new_feature = {"geometry": row["SHAPE"], "attributes": new_attributes}        
        else:
            new_feature = {"attributes": new_attributes}

        #logger.info("\n\n\n  records_to_add  {}".format(new_feature))
        records_to_add.append(new_feature)

        if index % batchSize == 0:
            logger.info("\n      Batch {}".format(index/batchSize))
            append_new_records(records_to_add, bCheck4Existing, globalids_to_add, targetLyrOrTbl, update=targetLyrOrTbl, track=None, operation="add", batch=batchSize, use_global_ids=True)        
            records_to_add = []
            globalids_to_add = []            

    logger.info("\n      Append remaining records")
    append_new_records(records_to_add, bCheck4Existing, globalids_to_add, targetLyrOrTbl, update=targetLyrOrTbl, track=None, operation="add", batch=batchSize, use_global_ids=True)


@run_update
def append_new_records(records_to_add, bCheck4Existing, globalids_to_add, targetLyrOrTbl):
    
    if not bCheck4Existing:
        return records_to_add
    else:
        # query for records that are not in the target
        s_globalids = ', '.join("'{0}'".format(g) for g in globalids_to_add)

        resp = targetLyrOrTbl.query("globalid in (" + s_globalids +")", out_fields=["globalid"], return_geometry=False, return_all_records = True)
        logger.info("\n      {} records already exist in target.".format(len(resp.features)))

        globalids_exist_in_target = []
        for feat in resp.features:
            globalids_exist_in_target.append(feat.attributes["globalid"]) 

        records_to_add_not_in_target = list(filter(lambda d: d["attributes"]["globalid"] not in globalids_exist_in_target, records_to_add))

        if len(records_to_add_not_in_target)>0:
            logger.info("\n  Records to add: {}".format(len(records_to_add_not_in_target)))
            return records_to_add_not_in_target
        else:
            logger.info("\n   All records exist in the target. No need to add again")


def updateTimeRan(file_last_time_run, startTimeInUTC):
    time_run = {
        "note": "The time is in UTC",
        "lastTimeStart": startTimeInUTC,
        "lastTimeEnd": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }
    file_last_time_run.seek(0)
    file_last_time_run.write(json.dumps(time_run,indent=4, default=str))
    file_last_time_run.truncate()
    file_last_time_run.close()
    
if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Get Logger & Log Directory
    logger, log_dir = get_logger(this_dir, this_filename, start_time)

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, "./config/config_append.json"))
    #the_portal = parameters["the_portal"]
    #gis = GIS(the_portal["path"], client_id=the_portal["client_id"])
    # token = the_portal['token']
    gis = GIS("pro")
    
    #file_last_time_run = open(os.path.join(this_dir, "./config/config_append_last_time_ran.json"), "r+")
    #last_time_run = json.load(file_last_time_run)        

    try:           
        #lastTimeRan = last_time_run["lastTimeStart"]
        #startTimeInUTC = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        batchSize = parameters["batchSize"]

        tasks = parameters["tasks"]
        
        for task in tasks:
            if task["skip"] == False:
                logger.info("\n\n\n *********** {} *************".format(task["description"]))
                task_start_time = time.time()

                processTask(task, batchSize)

                logger.info("\n\n\n ... task run time: {0} Minutes".format(round(((time.time() - task_start_time) / 60), 2)))
    
        #updateTimeRan(file_last_time_run, startTimeInUTC)

    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info("\n\n\nProgram Run Time: {0} Minutes".format(round(((time.time() - start_time) / 60), 2)))

