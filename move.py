import arcpy
import arcgis
from arcgis.gis import GIS

import zipfile
import json
import traceback
from datetime import datetime
import logging
import time
import json
import os
import sys

import smtplib
from email.message import EmailMessage

def get_config(in_file):

    with open(in_file) as config:
        param_dict = json.load(config)

    return param_dict


def get_logger(t_dir, t_filename, s_time):

    log = logging.getLogger(__name__)
    log.setLevel(1)

    # Set Logger Time
    logger_date = datetime.fromtimestamp(s_time).strftime("%Y_%m_%d")
    logger_time = datetime.fromtimestamp(s_time).strftime("%H_%M_%S")

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

def fetch_items(gis_source, item_list, dump_dir, run_time, sWhere):

    downloadedFilePaths = []
    for item_id in item_list:
        item = gis_source.content.search('id: {}'.format(item_id))
        if len(item) != 1:
            raise Exception('Ambiguous or Empty Set Returned from GIS')
        else:
            item = item[0]

            logger.info('to export file geodatabase for {}'.format(item.title))
            lyr_sWheres= []
            for x in range(4):
                lyr_sWheres.append(
                    {
                        "id": x,
                        "where": sWhere
                    }
                )
            exportParameters = {
                "layers": lyr_sWheres
            }
            logger.info("exportParameters: {}".format(exportParameters))
            
            s_exportParameters = json.dumps(exportParameters)
            fgdb_item = item.export('{}{}'.format(item.title, run_time), 'File Geodatabase',parameters = s_exportParameters)

            fgdb_path = fgdb_item.download(dump_dir)

            fgdb_item.delete()

            downloadedFilePaths.append(fgdb_path)

            logger.info('Extracted Item')
    
    return downloadedFilePaths


def get_dump_dir(this_dir, start_time):

    # Date & Time
    run_time = datetime.fromtimestamp(start_time).strftime('%m_%d_%H_%M')

    # Ensure Logs Directory Exists
    dump_dir = os.path.join(this_dir, 'extractions', run_time)
    if not os.path.exists(dump_dir):
        os.makedirs(dump_dir)

    return dump_dir, run_time


def updateTimeRan(file_last_time_run, startTimeInUTC):
    time_run = {
        "note": "The time is in UTC",
        "lastTimeStart": startTimeInUTC,
        "lastTimeEnd": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    file_last_time_run.seek(0)
    file_last_time_run.write(json.dumps(time_run, indent=4, default=str))
    file_last_time_run.truncate()
    file_last_time_run.close()


def send_email(email_config, dynamic_content):

    smtp_server = email_config["smtp_server"]    
    smtp_port = email_config["smtp_port"]
    from_email = email_config["from_email"]
    to_email = email_config["to_email"]    
    msg_subject = email_config["subject"]
    msg_content = email_config["content"]
    s_dynamic_content = "<br><br>".join(dynamic_content)
    msg_content = msg_content.replace("{dynamic_content}", s_dynamic_content)
  
    msg = EmailMessage()
    
    msg['From'] = from_email
    msg['To'] =  to_email
    msg['Subject'] = msg_subject
    logger.info("Email body: {}".format(msg_content))
    msg.set_content(msg_content, subtype='html')


    # Send the message via our own SMTP server.
    smtp = smtplib.SMTP(smtp_server, smtp_port)
    #smtp = smtplib.SMTP_SSL(smtp_server, 465)
    smtp.send_message(msg)
    smtp.quit()
    logger.info("\nEmail sent to {}".format(to_email))

    return True

def add_message(dynamic_content, logger, msg):    
    msg.replace("<br>", "\n")
    logger.info(msg)
    msg.replace("\n", "<br>")
    dynamic_content.append(msg)

if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Get Logger & Log Directory
    logger, log_dir = get_logger(this_dir, this_filename, start_time)

    # Get Dump Directory
    dump_dir, run_time = get_dump_dir(this_dir, start_time)

    logger.info("Python version {}".format(sys.version))
    logger.info("ArcGIS Python API version {}".format(arcgis.__version__))

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, "./config/config_move.json"))

    email_config = parameters["email_config"] 
    dynamic_content = []
    add_message(dynamic_content, logger, "Started to run at {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    the_portal_source = parameters["the_portal_source"]
    gis_source = GIS(the_portal_source['path'], the_portal_source['user'], the_portal_source['pass'])
    # token = the_portal['token']

    the_portal_target = parameters["the_portal_target"]
    gis_target = GIS(the_portal_target['path'], the_portal_target['user'], the_portal_target['pass'])

    file_last_time_run = open(os.path.join(this_dir, "./config/config_move_last_time_ran.json"), "r+")

    last_time_run = json.load(file_last_time_run)

    
    try:

        lastTimeRan = last_time_run["lastTimeStart"]

        source_item_id   = parameters['source_item_id']
        source_item = gis_source.content.get(source_item_id)
        target_item_id = parameters['target_item_id']    
        target_item = gis_target.content.get(target_item_id)

        startTimeInUTC = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # the where clause will be used to export data and delete data
        sWhere = "CreationDate <= TIMESTAMP '" + startTimeInUTC + "'" # CreationDate
        add_message(dynamic_content, logger, "SQL where used to export the data {}".format(sWhere))

        downloadedFilePaths = fetch_items(gis_source, [source_item_id], dump_dir, run_time, sWhere)

        add_message(dynamic_content, logger, "Item downloaded/backed up to {}".format(downloadedFilePaths))

        downloaded_fgdbZip = downloadedFilePaths[0]

        extracted_GDB = None
        with zipfile.ZipFile(downloaded_fgdbZip, 'r') as zip_ref:
            extracted_GDB = zip_ref.extractall(dump_dir)
        
        gdbFolder = None
        subfolders = [ f.path for f in os.scandir(dump_dir) if f.is_dir() ]
        if subfolders is None or len(subfolders) == 0:
            raise Exception('No subfolder found in the extraction')
        else:
            for fld in subfolders:
                if fld.lower().endswith("gdb"):
                    gdbFolder = fld

        if gdbFolder is not None:
            add_message(dynamic_content, logger, "The downloaded zip is extracted to {}".format(gdbFolder))
        else:
            raise Exception('No gdb folder found in the extraction')

        #arcpy.env.preserveGlobalIds = True
        tasks = parameters["tasks"]        
        for task in tasks:
            name = task["name"]
            dataType = task["dataType"]
            id_in_item = task["id_in_item"]

            add_message(dynamic_content, logger, "\n\n\n *********** To process {} *************".format(name))
            task_start_time = time.time()

            fc_ft_in_gdb = "{}\\{}".format(gdbFolder, name)
            num_rec_in_gdb = int(arcpy.GetCount_management(fc_ft_in_gdb)[0])
            msg = "Number of records in the gdb to append: {}".format(num_rec_in_gdb)
            add_message(dynamic_content, logger, msg)
            
            if num_rec_in_gdb == 0:
                msg = "No records to append"
                add_message(dynamic_content, logger, msg)
            else:
                if dataType == "LAYER":
                    lyr_tbl = target_item.layers[id_in_item]
                else:
                    lyr_tbl = target_item.tables[id_in_item]

                lyr_tbl_url = lyr_tbl.url

                # to append to target
                num_of_records_before = lyr_tbl.query("1=1", return_count_only=True)
                arcpy.management.Append(fc_ft_in_gdb, lyr_tbl_url, "TEST", None, '', '')
                num_of_records_after = lyr_tbl.query("1=1", return_count_only=True)
                msg = "Number of records in the target before appending: {} and after appending: {}".format(num_of_records_before, num_of_records_after)
                add_message(dynamic_content, logger, msg)
                
                # to delete the source
                if True:                
                    if dataType == "LAYER":
                        source_lyr_tbl = source_item.layers[id_in_item]
                    else:
                        source_lyr_tbl = source_item.tables[id_in_item]

                    num_of_records_before_deletion = int(arcpy.GetCount_management(source_lyr_tbl.url)[0])

                    # Execute SelectLayerByAttribute to determine which rows to delete
                    selectedRecords = arcpy.SelectLayerByAttribute_management(source_lyr_tbl.url, "NEW_SELECTION", sWhere)

                    # Execute GetCount and if some features have been selected, then execute
                    #  DeleteRows to remove the selected rows.
                    if int(arcpy.GetCount_management(selectedRecords)[0]) > 0:
                        arcpy.DeleteRows_management(selectedRecords)                    

                    num_of_records_after_deletion = int(arcpy.GetCount_management(source_lyr_tbl.url)[0])

                    msg = "Number of records in the source before deletion: {} and after deletion: {}".format(num_of_records_before_deletion, num_of_records_after_deletion)
                    add_message(dynamic_content, logger, msg)
                    
            logger.info("\n\n\n ... task run time: {0} Minutes".format(round(((time.time() - task_start_time) / 60), 2)))
        
        updateTimeRan(file_last_time_run, startTimeInUTC)

    except Exception:
        msg = traceback.format_exc()
        add_message(dynamic_content, logger, msg)
    finally:
        send_email(email_config, dynamic_content)
        # Log Run Time
        logger.info("\n\n\nProgram Run Time: {0} Minutes".format(round(((time.time() - start_time) / 60), 2)))
