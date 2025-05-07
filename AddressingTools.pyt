# -*- coding: utf-8 -*-
import os
import arcpy

class Toolbox:
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Addressing Toolbox"
        self.alias = "Addressing Toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [ProcessSitePlanImageTool, RemoveImageFromMosaicTool, PushAddressTool]


class ProcessSitePlanImageTool:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Process Site Plan Image Tool"
        self.description = "Clips a georeferenced site plan image file with a user-digitized polygon boundary, then " \
                           "appends that clipped raster dataset to the Site Plan mosaic dataset."

    def getParameterInfo(self):
        """Define the tool parameters."""
        param0 = arcpy.Parameter(
            displayName="Georeferenced site plan image layer",
            name="in_raster",
            datatype="GPRasterLayer",
            parameterType="Required",
            direction="Input"
        )

        param1 = arcpy.Parameter(
            displayName="AMANDA project number",
            name="project_number",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )

        param2 = arcpy.Parameter(
            displayName="Address Report ID",
            name="address_report_id",
            datatype="GPString",
            parameterType="Optional",
            direction="Input"
        )

        params = [param0, param1, param2]
        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return

    def execute(self, params, messages):
        """The source code of the tool."""
        raster_layer = params[0].valueAsText
        project_number = params[1].valueAsText
        address_report_id = params[2].valueAsText
        process_site_plan(raster_layer, project_number, address_report_id)
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return


class RemoveImageFromMosaicTool:
    def __init__(self):
        """Define the tool."""
        self.label = "Remove Image from Mosaic Tool"
        self.description = ("Removes an image from a mosaic dataset and deletes the associated raster file "
                            "from the mosaic file geodatabase.")

    def getParameterInfo(self):
        """Define the tool parameters."""
        # Parameter 0: Filepath to the mosaic dataset
        param0 = arcpy.Parameter(
            displayName="Mosaic dataset",
            name="mosaic_dataset",
            datatype="DEMosaicDataset",
            parameterType="Required",
            direction="Input"
        )

        # Parameter 1: Name of the image, value list populated from the 'Name' field in the mosaic dataset
        param1 = arcpy.Parameter(
            displayName="Name of image to remove",
            name="image_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )
        param1.filter.type = "ValueList"

        return [param0, param1]

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Update the value list for the image names by iterating through the Footprint table."""
        if parameters[0].value: # and not parameters[1].altered:
            mosaic_dataset = parameters[0].valueAsText
            if arcpy.Exists(mosaic_dataset):
                arcpy.ExportMosaicDatasetPaths_management(
                    in_mosaic_dataset=mosaic_dataset,
                    out_table=r"memory\mosaic_table",
                    export_mode="ALL",
                    types_of_paths="RASTER")
                with arcpy.da.SearchCursor("memory\mosaic_table", ["SourceOID", "Path"]) as cursor:
                    list_image_filepaths = [row[1] for row in cursor if row[0] != -1] # weird duplicate record in exported table
                    list_image_names = extract_image_name(list_image_filepaths)
                    parameters[1].filter.list = sorted(list_image_names)
            arcpy.Delete_management(r"memory\mosaic_table")
        return

    def execute(self, parameters, messages):
        """Main execution logic."""
        mosaic_dataset = parameters[0].valueAsText
        image_name = parameters[1].valueAsText

        # Confirm the raster file exists in the file geodatabase
        raster_name = f"clip_{image_name}"
        raster_full_path = os.path.join(os.path.dirname(mosaic_dataset), raster_name)
        if not arcpy.Exists(raster_full_path):
            arcpy.AddError(f"Raster file {image_name} not found in the file geodatabase.")
            return

        # Remove the image from the mosaic dataset
        arcpy.RemoveRastersFromMosaicDataset_management(in_mosaic_dataset=mosaic_dataset,
                                                        where_clause=f"Name = 'clip_{image_name}'",
                                                        update_boundary="UPDATE_BOUNDARY",
                                                        remove_items="REMOVE_MOSAICDATASET_ITEMS"
                                                        )
        arcpy.AddMessage(f"Removed clip_{image_name} from the mosaic dataset.")

        # Delete the raster file from the file geodatabase
        arcpy.Delete_management(raster_full_path)
        arcpy.AddMessage(f"Deleted raster file {raster_full_path}.")

        return


# THIS CLASS WILL CONTROL THE PROCESS FOR INTEGRATING GEOSPATIAL ADDRESS RECORDS WITH AMANDA PROJECTS. THIS INCLUDES
# PREPROCESSING NEWLY ADDED ADDRESS POINT FEATURES WITH THE FOLLOWING ATTRIBUTES...
#   1. BUILDING INSPECTION AREA
#   2. X/Y COORDINATES
#   3. PARCEL ID NUMBER
# ...FOLLOWED BY CALLING THE SNOCO_GIS_PROPERTY_INSERT FUNCTION FROM THE AMANDA SCD_AMANDA_PROD DATABASE

class PushAddressTool:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Push Addresses to AMANdA Tool"
        self.description = "Preprocess address features and push to AMANDA database for project creation."
        self.domain_dict = {"Prefix": "Addressing_Prefix",
                            "Street_Type": "Addressing_Street_Type",
                            "Unit_Type": "Addressing_Unit_Type",
                            "City": "Addressing_City",
                            "Status": "Addressing_Status",
                            "BIA": "BuildingInspectionArea"}

    def getParameterInfo(self):
        """Define the tool parameters."""
        param0 = arcpy.Parameter(
            displayName="Choose the database environment to run the tool",
            name="env",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )

        param0.filter.type = "ValueList"
        param0.filter.list = ["PROD", "PROD_TEST"]


        params = [param0]
        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return

    def execute(self, params, messages):
        """The source code of the tool."""

        # constants
        db = params[0].valueAsText
        sde_connection = f"\\\snoco\gis\plng\GDB_connections_PAG\SCD_GDBA\SCD_GDBA@SCD_GIS_{db}.sde"
        bia_fc = f"SCD_GIS_{db}.SCD_GDBA.PLANNING__PERMIT__BUILDING_INSPECTION_AREAS"
        user_list = ["SNOCO\\SCD_GIS_Addressing", "SNOCO\\PDSGISEditor"]

        # Check if address and site plan polygon layers exist in current Pro project.
        address_layer = check_for_layers('Address Points - Active')
        siteplan_layer = check_for_layers('Site Plan polygon clip')
        if not address_layer:
            arcpy.AddError(f"Address point layer not found in current map!")
        if not siteplan_layer:
            arcpy.AddError(f"Site Plan polygon layer not found in current map!")

        # Select address point features by intersecting with site plan polygon layer.
        if address_layer and siteplan_layer:
            arcpy.SelectLayerByLocation_management(in_layer=address_layer,
                                                   overlap_type='INTERSECT',
                                                   select_features=siteplan_layer,
                                                   selection_type='NEW_SELECTION')

        # update the building inspection area value for the selected address point features
        update_bia(sde_connection, address_fc, bia_fc, user_list, self.domain_dict) # FIXME: change input to address_lyr



        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return


def process_site_plan(raster_name, folder_name, address_report_id):
    # Set the workspace
    arcpy.env.workspace = \
        r"\\snoco\gis\plng\carto\Projects\Addressing\Addressing_Permit_Tech_Template\Mosaic\SitePlanMosaic.gdb"
    arcpy.env.overwriteOutput = True
    arcpy.env.outputCoordinateSystem = "SitePlan_clip"
    p = arcpy.mp.ArcGISProject('CURRENT')
    m = p.listMaps('Map')[0]
    layers = m.listLayers()

    # Define the input Site Plan polygon clip feature class
    polygon_layer_name = "Site Plan polygon clip"

    # Check if the Site Plan polygon clip feature class exists and if it has features:
    polygon_lyr_data_src = None
    arcpy.RecalculateFeatureClassExtent_management(
        in_features=polygon_layer_name,
        store_extent=False)
    for layer in layers:
        if layer == polygon_layer_name:
            arcpy.AddMessage(f"{polygon_layer_name} found in Table of Contents...")
            check_for_features(layer)
            polygon_lyr_data_src = layer.dataSource

    # Define the input raster layer that is georeferenced and ready to be clipped
    raster_layer = m.listLayers(raster_name)[0]
    if raster_layer is None:
        arcpy.AddError(f"The georeferenced raster layer {raster_name} cannot be found in the Table of Contents!")
        return

    # Make copy of georeferenced raster layer to memory and rename
    clip_in_raster_lyr = f"memory\\clip_{clean_raster_name(raster_layer.name)}"
    arcpy.CopyRaster_management(in_raster=raster_layer, out_rasterdataset=clip_in_raster_lyr)

    # Remove white spaces from the AMANDA project folder name input string
    clip_out_raster_lyr = clean_raster_name(folder_name)
    clip_out_raster_lyr = f"clip_{clip_out_raster_lyr}"

    # Clip the georeferenced raster layer with the Site Plan polygon clip feature class
    arcpy.AddMessage("Clipping the georeferenced site plan raster layer...")
    check_for_clipped_raster(clip_out_raster_lyr) # check if the clipped raster exists in the GDB, and delete if it is
    arcpy.Clip_management(in_raster=clip_in_raster_lyr,
                          out_raster=clip_out_raster_lyr,
                          rectangle="#",
                          in_template_dataset=polygon_layer_name,
                          nodata_value="#",
                          clipping_geometry="ClippingGeometry")

    # Add the clipped raster to the Site Plan mosaic dataset
    arcpy.AddMessage(f"Appending {clip_in_raster_lyr} to the mosaic dataset...")
    mosaic_dataset = "SitePlans"
    arcpy.AddRastersToMosaicDataset_management(in_mosaic_dataset=mosaic_dataset,
                                               raster_type="Raster Dataset",
                                               input_path=clip_out_raster_lyr,
                                               duplicate_items_action="OVERWRITE_DUPLICATES")

    # Update the Address Report ID column
    if address_report_id:
        with arcpy.da.UpdateCursor(in_table=mosaic_dataset, field_names=['Name', 'AddressReportID']) as cursor:
            for row in cursor:
                if row[0] == clip_out_raster_lyr:
                    row[1] = address_report_id
                cursor.updateRow(row)

    # Remove all existing polygon features from the Site Plan polygon clip feature class
    arcpy.AddMessage("Emptying the Site Plan polygon clip layer...")
    arcpy.DeleteFeatures_management(polygon_layer_name)

    return


def clean_raster_name(input_string):
    return input_string.replace("-", "_").replace(" ", "_")


def check_for_features(layer_name):
    polygon_count = arcpy.GetCount_management(layer_name)[0]
    if int(polygon_count) == 1:
        arcpy.AddMessage(f"The feature class {layer_name} has one feature...")
        return
    elif int(polygon_count) == 0:
        arcpy.AddError(
            f"No features found in {layer_name}! Please add a feature before running the tool...")
        return
    else:
        arcpy.AddWarning(
            f"More than one feature was found in {layer_name}. There should only be one feature available when running"
            f"this tool...")
        return


def check_for_clipped_raster(raster_name):
    raster_list = arcpy.ListRasters("*", "GRID")
    for raster in raster_list:
        if raster == raster_name:
            arcpy.Delete_management(raster)
    arcpy.AddMessage(f'{raster_name} found in mosaic geodatabase. Deleting now...')
    return


def extract_image_name(list_filepaths):
    list_image_names = []
    string_prefix = r"\clip_"
    for filepath in list_filepaths:
        start_index = filepath.find(string_prefix)
        if start_index != -1:
            image_name = filepath[start_index + len(string_prefix):]
            list_image_names.append(image_name)
    return list_image_names


def update_bia(sde_connection, address_fc_name, bia_fc_name, user_list, domain_dict):
    """
    Updates the address point feature class using a spatial join with the building inspection area polygon feature class
    to update the BIA attribute field in the address point feature class.
    :return: none
    """

    # Mapping from LABEL descriptions to BIA codes to use domain values
    label_to_code = {
        "Area 1": 1,
        "Area 2": 2,
        "Area 3": 3,
        "Area 4": 4,
        "Area 5": 5,
        "Area 6": 6,
        "Area 7": 7,
        "Area 8": 8
    }

    # Check if the addressing point feature class exists
    address_path = f"{sde_connection}\{address_fc_name}"
    bia_path = f"{sde_connection}\{bia_fc_name}"
    arcpy.AddMessage(f"address_path: {address_path}\nbia_path: {bia_path}")
    if check_feature_class_exists(sde_connection, address_fc_name):
        # Retrieve list of attribute fields from the BIA feature class
        fields_to_delete = [field.name for field in arcpy.ListFields(bia_path) if field.name != 'SHAPE'
                            and field.name != 'OBJECTID']
        fields_to_delete.extend(["Join_Count", "TARGET_FID"])  # These fields will be added by the spatial join

        # Perform spatial join with specified field mappings
        joined_fc = arcpy.SpatialJoin_analysis(
            target_features=address_path,
            join_features=bia_path,
            out_feature_class="memory/joined_output",
            join_type="KEEP_COMMON",
            match_option="INTERSECT"
        )
        arcpy.AddMessage("BIA polygon features joined to address point features...")

        with arcpy.da.UpdateCursor(joined_fc, ["BIA", "LABEL"]) as cursor:
            for row in cursor:
                # Set 'BIA' to the code corresponding to 'LABEL', defaulting to 0 if not found
                row[0] = label_to_code.get(row[1], 0)  # Default to 0 if no match found
                cursor.updateRow(row)
        arcpy.AddMessage("Address point features updated with BIA values...")

        # Delete the unwanted attribute fields from joined feature class
        arcpy.DeleteField_management(joined_fc, fields_to_delete)
        arcpy.AddMessage("Deleted joined attribute fields originating from BIA feature class...")

        # Delete the original address point feature class
        arcpy.Delete_management(address_path)
        arcpy.AddMessage("Deleted the address point feature class...")

        # Copy the updated in-memory feature class to the original location
        arcpy.CopyFeatures_management(joined_fc, address_path)
        arcpy.AddMessage("Copied over new address point feature class with updated BIA values...")

        # Reassign domains to attribute fields
        for key, value in domain_dict.items():
            arcpy.AssignDomainToField_management(address_path, key, value)
        arcpy.AddMessage("Assigned domains to attribute fields in address point feature class...")

        # Update privileges
        set_privileges(address_path, user_list)
        arcpy.AddMessage("Setting privileges...")

        arcpy.AddMessage("BIA attribute field updated successfully!")
    else:
        arcpy.AddMessage("Processing halted due to missing address point feature class!")
    return


def check_feature_class_exists(sde_connection, fc_name):
    """Checks if the feature class exists in the SDE database."""
    feature_class_path = f"{sde_connection}/{fc_name}"
    if arcpy.Exists(feature_class_path):
        arcpy.AddMessage(f"Feature class '{fc_name}' found.")
        return True
    else:
        arcpy.AddError(f"Feature class '{fc_name}' not found.")
        return False


def set_privileges(fc_path, user_list):
    if arcpy.Exists(fc_path):
        arcpy.AddMessage("Updating privileges for addressing points...")
        for user in user_list:
            arcpy.ChangePrivileges_management(fc_path, user, "GRANT", "GRANT")
        arcpy.AddMessage("Privileges updated successfully...")
    return

def check_for_layers(layer_name, map_name="Map"):
    '''
    Check for the presence of the layer in the current Pro project, based on the layer name
    :return: layer object
    '''
    aprx = arcpy.mp.ArcGISProject('CURRENT')
    map = aprx.listMaps(map_name)[0]
    for lyr in map.listLayers():
        if lyr.name.lower() == layer_name.lower():
            return lyr
    return None

def calculateXY():
    '''
    Calculate the X and Y coordinate values for address point features.
    :return:
    '''
    return

def getPID():
    '''
    Intersects address point features with parcel features to add the parcel ID number as an attribute to the address
    point features.
    :return:
    '''
    return

def preprocess_address_features():
    '''
    Updates address point feature attributes with building inspection area, X/Y coordinates, and parcel ID numbers.
    :return:
    '''
    return


def call_amanda_function():
    '''
    Calls the Snoco_GIS_Property_UpdatePID function from the AMANDA database
    :return: this will either be a 0 or true/false value to indicate the function completed successfully.
    '''
    return


# TESTING
# raster_name = "19-109012 Carlson SP.tif"
# folder_name = "19-109012"
# process_site_plan(raster_name, folder_name)
