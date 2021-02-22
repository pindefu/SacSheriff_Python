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

def createView(sourceItem, suffix, out_folder):
    # create a view
    view_title = sourceItem.title + suffix

    items = gis_target.content.search("title:" + view_title)
    if len(items) > 0:
        # # make sure the name of the view is unique
        # i = 0
        # while len(gis_target.content.search("title:" + view_title)) > 0:
        #     i = i + 1
        #     view_title = view_title + "_" + str(i)

        logger.info(" View exists: {}: {}".format(view_title, items[0].id))
        return items[0]
    else:
        logger.info(" Creating view: {}".format(view_title))
        flc = FeatureLayerCollection.fromitem(sourceItem)
        view_item = flc.manager.create_view(name=view_title, capabilities=flc.properties["capabilities"]) #'Query,Create,Update,Delete,Uploads,Editing,Sync,Extract')            
        logger.debug(" Created view {} id: {}".format(view_title, view_item.id))
        try:
            view_item.move(out_folder)
        except Exception:
            logger.info(" Unable to move view {} id: {} to folder".format(view_title, view_item.id, out_folder))
        return view_item

def createWebMap_from_template(mapTemplateItem, newWebMap, newTitle, out_folder):
    typeKeywords = ",".join(mapTemplateItem.typeKeywords)
    ext = mapTemplateItem.extent
    str_ext = "{},{},{},{}".format(ext[0][0], ext[0][1], ext[1][0], ext[1][1])
    webmap_item_properties = {'title': newTitle, 'typeKeywords': typeKeywords, 'extent': str_ext, 'snippet': mapTemplateItem.snippet, "tags": mapTemplateItem.tags}
    newWebMap_Item= newWebMap.save(webmap_item_properties, folder=out_folder)

    logger.debug(" Created web map: id {} with the same typekeywords, extent, bookmarks, snippet, and tags as the template web map.".format(newWebMap_Item.id))
    return newWebMap_Item  

def getMainTextOfName(lyrTitle):
    lyrTitle=lyrTitle.replace(" ", "")
    lyrTitle=lyrTitle.replace("-", "")
    lyrTitle=lyrTitle.replace("_", "")
    lyrTitle=lyrTitle.lower()
    return lyrTitle

def findSourceItem(lyrTitleKey, sourceItems):
    sourceItem = None
    # find the source layer
    if lyrTitleKey in sourceItems:
        sourceItem=sourceItems[lyrTitleKey]
        logger.info("---- Found the source item {}".format(sourceItem))
    else:
        logger.info("---- Unable to find the source item of {}".format(lyrTitleKey))
    return sourceItem

# Keep the visible fields
# Remove fields that are not in the view.
# Update fields that have changed cases (upper or lower)
# Update field aliases
def updatePopupInfo(templateLayer_data, view_item, lyrIdInService):

    popupInfo = templateLayer_data["popupInfo"]
    popupFieldInfos = popupInfo["fieldInfos"]

    view_layer = FeatureLayerCollection.fromitem(view_item).layers[lyrIdInService]
    view_fields = view_layer.properties["fields"]
    view_fieldNames = [ sub['name'].lower() for sub in view_fields ] 

    # Keep the visible fields
    visiblePopupFieldInfos = list(filter(lambda d: d['visible'] == True, popupFieldInfos))
    # Get rid of the fields that are not in the list of the view fields (ignore case)
    popupFieldInfos = list(filter(lambda d: d['fieldName'].lower() in view_fieldNames, visiblePopupFieldInfos))
    # Log the fileds that are not in the view anymore
    fields_removed = list(filter(lambda d: d['fieldName'].lower() not in view_fieldNames, visiblePopupFieldInfos))
    logger.info("  Fields removed from the popup {}".format(fields_removed))

    logger.info(" Combine the popup fields with their aliases from the source")
    for f in popupFieldInfos:
        fieldName = f["fieldName"]        
        # search for the field, ignore case this time.
        fld = next(item for item in view_fields if item["name"].lower() == fieldName.lower()) 
        f["fieldName"] = fld['name']
        f["label"] = fld['alias']
        logger.info("    Updating field {} alias {}".format(fld['name'], f["label"]))

    logger.info("\n\n Visible fields to shown in pop-up: {} ".format(popupFieldInfos))

    # append the rest fields from the view to the popup.
    fields_alreadyInPopup = [ sub['fieldName'].lower() for sub in popupFieldInfos ] 
    fields_notShownInPopup = list(filter(lambda d: d['name'].lower() not in fields_alreadyInPopup, view_fields))
    
    logger.info("\n\n Appending fileds that are not visible: {} ".format(fields_notShownInPopup))
    for f in fields_notShownInPopup:
        fp = {
                "fieldName": f["name"],
                "visible": False,
                "isEditable": False,
                "label": f["alias"]
        }
        popupFieldInfos.append(fp)    
    
    logger.info(" Popup info updated in the memory (not yet committed)")
    popupInfo["fieldInfos"] = popupFieldInfos
    return popupInfo


# Copy the pop-up info. 
# Remove fields that are not in the view.
# Update fields that have changed cases (upper or lower)
# Update field aliases
def copyLayerTemplateToView(lyr, view_item, lyrIdInService):
    templateItemId = lyr["itemId"]
    templateItem = gis_template.content.get(templateItemId)
    templateItem_data = templateItem.get_data()

    template_lyr_url = lyr["url"]
    template_lyr_id = int(template_lyr_url.rsplit('/', 1)[1])

    templateLayer_data = next((sub for sub in templateItem_data["layers"] if sub['id'] == template_lyr_id), None) 
    #pop_up_definition = templateLayer_data['popupInfo']
    
    # replace the id with the id of the layer in the source view
    templateLayer_data['id'] = lyrIdInService

    popupFieldInfos = updatePopupInfo(templateLayer_data, view_item, lyrIdInService)
    
    #drawingInfo = view_item.layers[lyrIdInService].properties["drawingInfo"]
    #logger.info("  Got the drawing info from the source layer")

    updated_layer_data = {
                "id": lyrIdInService,
                "popupInfo": popupFieldInfos,
                "layerDefinition":{
                    #"drawingInfo":drawingInfo,
                    "defaultVisibility": True
                }
            }

    new_item_data = view_item.get_data()    
    if new_item_data == None or not "layers" in new_item_data:
        new_item_data = {
            "layers": [updated_layer_data]
        }
    else:
        # Does the layer already exist, if yes, replace, if not, append
        blyrDefinitionReplaced = False
        for ly in new_item_data["layers"]:
            if ly["id"] == lyrIdInService:                
                # replace the layer's definition
                #ly["popupInfo"] = popupFieldInfos
                #ly["layerDefinition"] = updated_layer_data["layerDefinition"]

                ly = updated_layer_data

                blyrDefinitionReplaced = True
                break
        if not blyrDefinitionReplaced:
            new_item_data["layers"].append(updated_layer_data)
            
    
    logger.info(" Build the view properties to include the desired popup and layer definition")
    item_properties = {"text": json.dumps(new_item_data), "tags": sourceItem.tags}
    
    # 'Commit' the updates to the view item
    view_item.update(item_properties=item_properties)
    logger.info(" committed the updates in view properties {}".format(view_item.id))
    return view_item   


def createWebMap_from_template_and_new_source_layers(mpConfig, sourceItems, sourceItemsWithMultipleLayers, layers_not_to_create_views, map_and_view_name_suffix):
    logger.info("\n\n\n -------- {} -----------".format(mpConfig["title"]))
    # "title": "Climber",      
    # "mapTemplateItemId": "26e8c2cd26b742cea0091dfcfef7c030",  
    
    viewSuffix = "_" + mpConfig["title"] + "_View"
    newTitle = mpConfig["title"] + " Web Map " + map_and_view_name_suffix
    out_folder = map_and_view_name_suffix + "_" + mpConfig["title"]
    gis_target.content.create_folder(out_folder)

    # find the template web map
    mapTemplateItem = gis_template.content.get(mpConfig["mapTemplateItemId"])    
    newWebMap = WebMap(mapTemplateItem)
    logger.info(" Found the tempalte web map {} {}".format(mapTemplateItem.title,mapTemplateItem.id))

    viewsExisted={}
    logger.info(" Next: Loop through each layer in the web map. Keep its popup info, but replace its symbols with the new source item")
    # replace the layers in the template web map with views from the new source, but keep the style and pop-up in the web map
    for lyr in newWebMap.layers:
        logger.info("\n\nWorking on layer {} in the tempalte web map".format(lyr.title))

        lyrTitleKey = getMainTextOfName(lyr.title)
        sourceItem = findSourceItem(lyrTitleKey, sourceItems)
        if sourceItem is None:
            logger.info(" This layer will be re-used (because no new sources are defined in the config)")
            continue

        lyrIdInService = 0
        if len(sourceItem.layers) > 1:
            lyrIdInService = sourceItemsWithMultipleLayers[lyrTitleKey]       

        if lyrTitleKey not in layers_not_to_create_views:
            logger.info(" Need to create the view or find the view existed.")
            if sourceItem.id in viewsExisted:
                view_item = viewsExisted[sourceItem.id]
            else:
                view_item = createView(sourceItem, viewSuffix, out_folder)
                viewsExisted[sourceItem.id] = view_item
            
            new_layer_in_webmap = copyLayerTemplateToView(lyr, view_item, lyrIdInService)
        else:
            logger.info(" No need to create a views. Just point the layer to the source")
            new_layer_in_webmap = sourceItem

        lyr.itemId = new_layer_in_webmap.id
        lyr.url = new_layer_in_webmap.layers[lyrIdInService].url
        logger.info(" Updated the web map to use the layer id {} and url {} from the new view or new source item".format(lyr.itemId, lyr.url))

        # Some layers (e.g., Callback) have popupInfo in the web map. Update their fields as well.
        if "popupInfo" in lyr:
            logger.info(" This layer has popupInfo in the web map")
            logger.info(" Updating popupInfo in the web map")
            lyr.popupInfo = updatePopupInfo(lyr, view_item, lyrIdInService)

    createWebMap_from_template(mapTemplateItem, newWebMap, newTitle, out_folder)

if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Get Logger & Log Directory
    logger, log_dir = get_logger(this_dir, this_filename, start_time)

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, "./config/config_webmaps.json"))
    target_portal = parameters["target_portal"]
    template_portal = parameters["template_portal"]

    if template_portal == "pro":
        gis_template = GIS("pro")
    else:
        gis_template =  GIS(template_portal["path"], client_id=template_portal["client_id"])

    if target_portal == "pro":
        gis_target = GIS("pro")
    else:
        gis_target =  GIS(target_portal["path"], client_id=target_portal["client_id"])

    try:
        layerItemIds = parameters["layerItemIds"]

        sourceItems={}
        for sourceId in layerItemIds:
            sourceItem = gis_target.content.get(layerItemIds[sourceId])
            if sourceItem is None:
                logger.info("current time: {}".format(time.strftime("%H:%M:%S", time.localtime())))
                logger.error("Unable to find the source item {}: {}".format(sourceId, layerItemIds[sourceId]))
                logger.error("Exiting ...")
                sys.exit(1)
            sourceItems[getMainTextOfName(sourceId)] = sourceItem

        sourceItemsWithMultipleLayers_orig = parameters["sourceItemsWithMultipleLayers"]
        sourceItemsWithMultipleLayers = {}
        for k in sourceItemsWithMultipleLayers_orig:
            sourceItemsWithMultipleLayers[getMainTextOfName(k)] = sourceItemsWithMultipleLayers_orig[k]

        layers_not_to_create_views = parameters["layers_not_to_create_views"]
        map_and_view_name_suffix = parameters["map_and_view_name_suffix"]

        webmaps_to_create = parameters["webmaps_to_create"]        
        for mpConfig in webmaps_to_create:
            if mpConfig["skip"] == False:
                createWebMap_from_template_and_new_source_layers(mpConfig, sourceItems, sourceItemsWithMultipleLayers, layers_not_to_create_views, map_and_view_name_suffix)

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

