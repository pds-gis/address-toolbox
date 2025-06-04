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


class PushAddressTool:
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Push Address Records to AMANDA"
        self.description = "Preprocess address features and push to AMANDA database for project creation."
        self.amanda_dict = {'PropertyRSN': 'RSN'
                            ,'PropCode': '1'
                            ,'PropHouse': 'House_Number'
                            ,'PropStreetPrefix': 'Prefix'
                            ,'PropStreet': 'Street_Name'
                            ,'PropStreetType': 'Street_Type'
                            ,'PropStreetDirection': 'Direction'
                            ,'PropUnitType': 'Unit_Type'
                            ,'PropUnit': 'Unit_Number'
                            ,'PropCity': 'City'
                            ,'PropProvince': 'State'
                            ,'PropPostal': 'Zip_Code'
                            ,'PropPlan': 'Plat'
                            ,'PropLot': 'Lot'
                            ,'PropBlock': 'Block'
                            ,'PropSubDivision': 'Subdivision'
                            ,'StatusCode': 1
                            ,'PropSection': 'Section'
                            ,'PropTownship': 'Township'
                            ,'PropRange': 'Range'
                            ,'PropertyRoll': 'PDS_Project_ID'
                            ,'DateCreated': 'created_date'
                            ,'StampUser': 'last_edited_user'
                            ,'StampDate': 'last_edited_date'
                            ,'PropGisId1': 'Parcel_ID'
                            ,'PropX': 'X'
                            ,'PropY': 'Y'
                            ,'BuildingInspectionArea': 'BIA'}

    def getParameterInfo(self):
        """Define the tool parameters."""
        param0 = arcpy.Parameter(
            displayName="Choose the AMANDA database environment to target",
            name="amanda_server",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )
        param0.filter.type = "ValueList"
        param0.filter.list = ["PAG-AMANDA", "DAG-AMANDA"]
        param0.value = "PAG-AMANDA"

        param1 = arcpy.Parameter(
            displayName="Choose the address point layer",
            name="address_lyr",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )

        param2 = arcpy.Parameter(
            displayName="Choose the GIS server environment",
            name="sde_env",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )
        param2.filter.type = "ValueList"
        param2.filter.list = ["PROD", "PROD_TEST"]
        param2.value = "PROD"

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

        # constants
        server_name = params[0].valueAsText
        address_lyr = params[1].valueAsText
        db = params[2].valueAsText
        sde_connection = f"\\\snoco\gis\plng\GDB_connections_PAG\SCD_GDBA\SCD_GDBA@SCD_GIS_{db}.sde"
        bia_fc = f"SCD_GIS_{db}.SCD_GDBA.PLANNING__PERMIT__BUILDING_INSPECTION_AREAS"

        # test mode
        test_mode = True # change to True when testing in PyCharm

        # double-check that the address layer has selected points
        if not test_mode:
            desc = arcpy.Describe(address_lyr)
            if hasattr(desc, "FIDSet") and not desc.FIDSet:
                raise arcpy.ExecuteError("No features selected in the address layer! Please select features and try again...")
            arcpy.AddMessage("Proceeding with selected address point features...")

            # remove selected address point features where RSN attribute value is not null
            if check_for_nulls(input_layer=address_lyr, attrb_field='RSN'):
                arcpy.SelectLayerByAttribute_management(in_layer_or_view=address_lyr,
                                                        selection_type="REMOVE_FROM_SELECTION",
                                                        where_clause="RSN IS NOT NULL")

        # update the building inspection area value for the selected address point features
        update_bia(sde_connection, address_lyr, bia_fc)


        # calculate the x and y coordinate for the selected address point features
        calc_xy(address_lyr)

        # intersect selected address point features with parcels to update parcel ID values.
        update_pid(address_lyr)

        # based on the PID for the selected addresses, populate records in AMANDA and get returned RSN value
        execute_amanda_sproc(server=server_name, address_layer=address_lyr)

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

def update_bia(sde_connection, address_layer, bia_fc_name):
    """Updates the address point feature class using a spatial join with the building inspection area polygon feature class
    to update the BIA attribute field in the address point feature class."""

    # Check if the building inspection area feature class exists.
    bia_path = f"{sde_connection}\{bia_fc_name}"
    arcpy.AddMessage(f"bia_path: {bia_path}")
    joined_lyr = "memory\\joined_address_bia"
    if check_feature_class_exists(sde_connection, bia_fc_name):
        # Perform spatial join with specified field mappings
        arcpy.SpatialJoin_analysis(
            target_features=address_layer,
            join_features=bia_path,
            out_feature_class=joined_lyr,
            join_operation="JOIN_ONE_TO_ONE",
            match_option="INTERSECT"
        )

    # Transfer attributes based on spatial join
    from_field = 'BIA'
    to_field = 'BIA'
    joined_dict = transfer_attributes_spatial_join(address_layer, joined_lyr, from_field, to_field)
    arcpy.AddMessage(f"{len(joined_dict)} address points successfully updated with BIA values.")
    return

def update_pid(address_layer):
    '''Spatially joins address point features with parcel features to add the parcel ID number as an attribute to the
    address point features.'''

    parcel_fc = r'\\snoco\gis\plng\GDB_connections_PAG\WIN_USER\WIN_USER@GIS_FEATURES_PAG.sde\GDBA.CADASTRAL__parcels'
    joined_lyr = "memory\\joined_address_parcel"
    arcpy.SpatialJoin_analysis(
        target_features=address_layer,
        join_features=parcel_fc,
        out_feature_class=joined_lyr,
        join_operation="JOIN_ONE_TO_ONE",
        match_option="INTERSECT"
    )

    # Transfer attributes based on spatial join
    from_field = 'PARCEL_ID'
    to_field = 'Parcel_ID'
    joined_dict = transfer_attributes_spatial_join(address_layer, joined_lyr, from_field, to_field)
    arcpy.AddMessage(f"{len(joined_dict)} address points successfully updated with parcel ID values.")
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

def transfer_attributes_spatial_join(target_layer, joined_layer, from_field, to_field):
    '''
    Will transfer values from an attribute field in a temporary spatially joined layer to an attribute field in the
    target layer.
    '''
    # Build dictionary from joined layer: {object_id: from_value}
    joined_dict = {}
    fields = [f.name for f in arcpy.ListFields(joined_layer)]
    from_field_clean = [f for f in fields if f.startswith(from_field) and f != from_field][0]  # e.g. "from_field_1"
    with arcpy.da.SearchCursor(
        in_table=joined_layer, field_names=["TARGET_FID", from_field_clean]) as cursor:
        for row in cursor:
            joined_dict[row[0]] = row[1]

    # Update the address layer using the dictionary
    with arcpy.da.UpdateCursor(in_table=target_layer,field_names=["OBJECTID", to_field]) as cursor:
        for row in cursor:
            object_id = row[0]
            if object_id in joined_dict:
                row[1] = joined_dict[object_id]
            cursor.updateRow(row)
    return joined_dict


def calc_xy(address_layer):
    with arcpy.da.UpdateCursor(address_layer, ["X", "Y"]) as cursor:
        for row in cursor:
            row[0] = "SHAPE@X"
            row[1] = "SHAPE@Y"
            cursor.updateRow(row)
    arcpy.AddMessage("X and Y coordinates updated...")
    return


def check_for_nulls(input_layer, attrb_field):
    '''
    This function checks whether the input layer has attributes in the attribute field that are null.
    :param input_layer: Layer with selected features
    :param attrb_field: Name of the attribute field in the input layer that will be checked for nulls
    :return:
    '''
    # Construct a SQL statement using the attrb_field variable
    sql_clause = f"{attrb_field} IS NULL"

    with arcpy.da.SearchCursor(input_layer, attrb_field, sql_clause) as cursor:
        for row in cursor:
            if row[0] is None:
                return True

    return False


def get_amanda_cursor(server, database):
    '''

    :return:
    '''
    import pyodbc
    try:
        conn_amanda = pyodbc.connect('DRIVER={SQL Server Native Client 11.0;'
                                     f'SERVER={server};'
                                     f'DATABASE={database};'
                                     'Trusted_Connection=yes')
        return conn_amanda.cursor()
    except pyodbc.Error as ex:
        arcpy.AddError(f"An error occurred in SQL Server: {ex}")
        return


def execute_amanda_sproc(self, server, database, address_layer):
    '''
    Calls the Snoco_GIS_Property_Proc stored procedure from the AMANDA database.
    :return: this will be a string representing the RSN value generated by the AMANDA stored procedure.
    '''
    import pyodbc

    conn_string = f"DRIVER=SQL Server Native Client 11.0;SERVER={server};DATABASE={database};Trusted_Connection=yes"
    amanda_dict_fields = self.amanda_dict
    address_fields = list(amanda_dict_fields.values())

    with arcpy.da.SearchCursor(address_layer, address_fields) as cursor:
        for row in cursor:
            try:
                update_dict = dict(zip(address_fields, row))

                sproc_params = [
                    update_dict.get('House_Number'),
                    update_dict.get('1'),
                    update_dict.get('Prefix'),
                    update_dict.get('Street_Name'),
                    update_dict.get('Street_Type'),
                    update_dict.get('Direction'),
                    update_dict.get('Unit_Type'),
                    update_dict.get('Unit_Number'),
                    update_dict.get('City'),
                    update_dict.get('State'),
                    update_dict.get('Zip_Code'),
                    update_dict.get('Plat'),
                    update_dict.get('Lot'),
                    update_dict.get('Block'),
                    update_dict.get('Subdivision'),
                    update_dict.get(1),
                    update_dict.get('Section'),
                    update_dict.get('Township'),
                    update_dict.get('Range'),
                    update_dict.get('PDS_Project_ID'),
                    update_dict.get('created_date'),
                    update_dict.get('last_edited_user'),
                    update_dict.get('last_edited_date'),
                    update_dict.get('Parcel_ID'),
                    update_dict.get('X'),
                    update_dict.get('Y'),
                    update_dict.get('BIA'),
                ]

                with pyodbc.connect(conn_string) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        DECLARE @outputRSN INT;
                        EXEC Snoco_GIS_Property_Proc
                            @PropHouse=?, @PropStreetPrefix=?, @PropStreet=?, @PropStreetType=?, @PropStreetDirection=?,
                            @PropUnitType=?, @PropUnit=?, @PropCity=?, @PropProvince=?, @PropPostal=?,
                            @PropPlan=?, @PropLot=?, @PropBlock=?, @PropSubDivision=?, @PropSection=?,
                            @PropTownship=?, @PropRange=?, @PropertyRoll=?, @DateCreated=?, @StampUser=?,
                            @StampDate=?, @PropGisId1=?, @PropX=?, @PropY=?, @BuildingInspectionArea=?,
                            @PropertyRSN=@outputRSN OUTPUT;
                        SELECT @outputRSN;""",
                    sproc_params)
                    property_rsn = cursor.fetchone()[0]

                if property_rsn == -1:
                    arcpy.AddWarning("Property already exists. Skipping.")
                else:
                    arcpy.AddMessage(f"Pushed property successfully. RSN: {property_rsn}")

                return property_rsn

            except Exception as e:
                arcpy.AddWarning(f"Error processing record: {str(e)}")


# TESTING PushAddressTool
class TestParameter:
    def __init__(self, value):
        self.valueAsText = value

class FakeMessages:
    def AddMessage(self, msg):
        print(f"[MESSAGE] {msg}")
    def AddWarning(self, msg):
        print(f"[WARNING] {msg}")
    def AddError(self, msg):
        print(f"[ERROR] {msg}")

def _test_execute():
    amanda_server = 'PAG-AMANDA'
    address_fc = r'\\snoco\gis\plng\GDB_connections_PAG\SCD_GDBA\SCD_GDBA@SCD_GIS_PROD_TEST.sde\SCD_GDBA.ADDRESSING__Address_Points_PDS_TEST'
    address_lyr = arcpy.MakeFeatureLayer_management(address_fc, 'address_layer')
    sde_env = 'PROD_TEST'
    test_params = [
        TestParameter(amanda_server),
        TestParameter(address_lyr),
        TestParameter(sde_env)
        ]

    fake_messages = FakeMessages()
    tool = PushAddressTool()
    tool.execute(test_params, fake_messages)
    return

_test_execute()