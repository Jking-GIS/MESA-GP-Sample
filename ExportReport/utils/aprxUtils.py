import arcpy
import copy
import os
import json
import re
import requests
import time
import math
import datetime

class AprxUtils(object):
    def __init__(self, base_utils):
        self.aprx = None

        self.map_obj = None

        self.cover_lyt = None
        self.content_lyt = None

        self.aoi_infos = {}
        self.aoi_layer = None
        self.aoi_source = None

        self.original_data_sources = {}

        self.new_project_idx = 0

        # self.map_finishing_idx = 0

        self.base_utils = base_utils
        
    def copy_new_project(self, new_project=None):
        if new_project is None and self.new_project_idx == 0:
            new_project = os.sep.join([self.base_utils.root_dir, '{}.aprx'.format(self.base_utils.config['project_name'])])
        else:
            new_project = os.sep.join([
                self.base_utils.root_dir, '{0}_{1}.aprx'.format(self.base_utils.config['project_name'], self.new_project_idx)
            ])

        self.aprx.saveACopy(new_project)
        self.open_aprx(new_project)
        self.new_project_idx += 1

    # open new aprx and reset global layout/map objects
    def open_aprx(self, aprx_url=None):
        if aprx_url is None:
            aprx_url = os.sep.join([self.base_utils.resources_dir, '{}.aprx'.format(self.base_utils.config['project_name'])])

        try:
            self.aprx = arcpy.mp.ArcGISProject(aprx_url)
        except (RuntimeError, TypeError, ValueError):
            self.base_utils.add_warning_statement('WARNING: Could not load aprx at {}'.format(aprx_url))

    def get_map_from_aprx(self, name):
        for map_obj in self.aprx.listMaps(name):
            return map_obj
        return None

    def get_map_from_lyt_cfg(self, lyt_cfg, name='map'):
        if name in lyt_cfg:
            return self.get_map_from_aprx(lyt_cfg[name])
        return None

    def get_all_mapframes(self, name='map'):
        mapframe_list = []
        for lyt in self.aprx.listLayouts():
            for mapframe in lyt.listElements('MAPFRAME_ELEMENT', self.base_utils.config['map_frames'][name]):
                mapframe_list.append(mapframe)
        return mapframe_list

    def get_layout_from_aprx(self, name):
        for lyt in self.aprx.listLayouts(name):
            return lyt
        return None

    # get map frame from layout
    def get_mapframe_from_layout(self, lyt, name='map'):
        for mapframe in lyt.listElements('MAPFRAME_ELEMENT', self.base_utils.config['map_frames'][name]):
            return mapframe
        return None

    # get map extent from layout
    def get_map_extent_from_layout(self, lyt, name='map'):
        for mapframe in lyt.listElements('MAPFRAME_ELEMENT', self.base_utils.config['map_frames'][name]):
            return mapframe.camera.getExtent()
        return None

    # sets the extent of all map frame elements in the given layout to the given extent
    def set_map_extent(self, map_frame, extent):
        try:
            map_frame.camera.setExtent(extent)
        except (RuntimeError, TypeError, NameError):
            self.base_utils.add_warning_statement('WARNING: Could not set map extent')

    # swap sources with template layers in the aprx
    # (this is done to preserve labelling and symbolization)
    def swap_sources_with_template(self, template, layer, map_obj):
        try:
            template.updateConnectionProperties(template.connectionProperties, layer.connectionProperties)
            map_obj.removeLayer(layer)

            return template
        except:
            self.base_utils.add_warning_statement('Could not swap sources for {}'.format(layer.name))
            return layer

    def add_layer_to_map_from_json(self, in_json, feature_name, map_obj, position='AUTO_ARRANGE'):
        try:
            feature_name = self.simplify_layer_name(feature_name)
            json_file_url = os.sep.join([self.base_utils.root_dir, 'json', '{}.json'.format(feature_name)])
            with open(json_file_url, 'w') as outfile:
                json.dump(in_json, outfile)
            shp = self.get_local_source_by_name(feature_name)
            arcpy.JSONToFeatures_conversion(json_file_url, shp)
            return self.make_lyrx_from_feature(shp, feature_name, map_obj, position)
        except (RuntimeError, TypeError, ValueError):
            self.base_utils.add_warning_statement('WARNING: Could not add layer to map from JSON')
            return None
    
    def make_lyrx_from_feature(self, shp, feature_name, map_obj, position='AUTO_ARRANGE'):
        try:
            arcpy.management.MakeFeatureLayer(shp, feature_name)
            lyrx = os.sep.join([self.base_utils.root_dir, 'lyrx', '{}.lyrx'.format(feature_name)])
            arcpy.management.SaveToLayerFile(feature_name, lyrx)
            lf = arcpy.mp.LayerFile(lyrx)
            added_layer = map_obj.addLayer(lf, position)[0] # add the layer file to the map
            return added_layer
        except (RuntimeError, TypeError, ValueError):
            self.base_utils.add_warning_statement('WARNING: Could not make lyrx for {}'.format(feature_name))
            return None

    def make_local_source(self, layer_name, map_obj, url_source):
        try:
            layer = self.get_layer_from_map(layer_name, map_obj)
            simplified_layer_name = self.simplify_layer_name(layer.name)
            local_source = self.get_local_source_by_name(simplified_layer_name)
            if arcpy.Exists(local_source):
                local_layer = map_obj.addDataFromPath(local_source)
                swapped_layer = self.swap_sources_with_template(layer, local_layer, map_obj)
                return swapped_layer

            buffered_extent = None
            try: # running from Pro
                buffered_extent = self.buffer_extent_by_parts(
                    self.base_utils.in_extent.XMin,
                    self.base_utils.in_extent.YMin,
                    self.base_utils.in_extent.XMax,
                    self.base_utils.in_extent.YMax,
                    100
                )
            except: # running from Service
                in_extent_split = self.base_utils.in_extent.split(' ')
                if len(in_extent_split) >= 4:
                    buffered_extent = self.buffer_extent_by_parts(
                        in_extent_split[0],
                        in_extent_split[1],
                        in_extent_split[2],
                        in_extent_split[3],
                        100
                    )

            envelope_string = None
            if buffered_extent:
                envelope_string = '{0},{1},{2},{3}'.format(
                    buffered_extent[0],
                    buffered_extent[1],
                    buffered_extent[2],
                    buffered_extent[3]
                )

            payload = {
                'f': 'json',
                'outFields': '*',
                'where': '1=1',
                'inSR': '102100'
            }
            if envelope_string:
                payload['geometry'] = envelope_string
                payload['geometryType'] = 'esriGeometryEnvelope'
            if self.base_utils.token:
                payload['token'] = self.base_utils.token

            req = requests.post(url_source + '/query', data=payload, verify=False)
            req_json = req.json()

            if 'error' in req_json and req_json['error']['code'] == 498:
                del payload['token']
                req = requests.post(url_source + '/query', data=payload, verify=False)
                req_json = req.json()
            
            local_layer = self.add_layer_to_map_from_json(req_json, simplified_layer_name, map_obj)
            swapped_layer = self.swap_sources_with_template(layer, local_layer, map_obj)
            return swapped_layer
        except:
            return None

    def get_local_source_by_name(self, name):
        return os.sep.join([self.base_utils.root_dir, '{}.gdb'.format(self.base_utils.config['gdb_name']), name])

    def get_layer_source(self, layer):
        layer_name = self.simplify_layer_name(layer.name)
        source = self.get_local_source_by_name(layer_name)
        return source

    def get_layer_count(self, layer):
        source = self.get_layer_source(layer)
        try:
            count = 0
            for row in arcpy.da.SearchCursor(source, '*'):
                count += 1
            return count
        except:
            return 0

    def get_all_oids_for_layer(self, layer, aoi, sorting_field=None):
        source = self.get_layer_source(layer)
        oids = []

        fields = ['objectid']
        if sorting_field:
            fields.append(sorting_field)

        for row in arcpy.da.SearchCursor(source, fields):
            oids.append(row)

        if sorting_field:
            oids.sort(key=lambda oids: oids[1], reverse=True)
        oids = map(lambda a: a[0], oids)

        return oids

    def get_layer_from_map(self, name, map_obj):
        for layer in map_obj.listLayers(name):
            return layer
        return None

    def get_table_from_map(self, name, map_obj):
        for table in map_obj.listTables(name):
            return table
        return None

    def toggle_layer_visibility(self, name, map_obj):
       for layer in map_obj.listLayers(name):
           layer.visible = not layer.visible

    def simplify_layer_name(self, name):
        return re.sub('\(|\)| ', '_', name)

    def clip_layer(self, layer, aoi_layer, map_obj, visible_fields, dissolve_field):
        source = self.get_layer_source(layer)
        clipped_source = '{} (CLIPPED)'.format(source)
        simplified_clipped_source = self.simplify_layer_name(clipped_source)
        arcpy.Clip_analysis(source, aoi_layer.dataSource, simplified_clipped_source)
        dissolved_source = '{} (CLIPPED_DISSOLVED)'.format(source)
        simplified_dissolved_source = self.simplify_layer_name(dissolved_source)
        arcpy.Dissolve_management(
            simplified_clipped_source,
            simplified_dissolved_source,
            dissolve_field,
            visible_fields,
            'MULTI_PART' 
        )
        for field in visible_fields:
            arcpy.AlterField_management(
                simplified_dissolved_source,
                '{0}_{1}'.format(field[1], field[0]),
                field[0],
            )
        clipped_layer = map_obj.addDataFromPath(simplified_dissolved_source)
        swapped_clipped_layer = self.swap_sources_with_template(layer, clipped_layer, map_obj)
        swapped_clipped_layer.name = '{} (CLIPPED_DISSOLVED)'.format(swapped_clipped_layer.name)
        return swapped_clipped_layer

    def copy_extent(self, mapframe, extent):
        copy_extent = mapframe.camera.getExtent()
        copy_extent.XMin = extent.XMin
        copy_extent.YMin = extent.YMin
        copy_extent.XMax = extent.XMax
        copy_extent.YMax = extent.YMax
        return copy_extent

    def set_extent_from_input(self, lyt, name='map', buffer_percent=0):
        mapframe = self.get_mapframe_from_layout(lyt, name)

        try: # running from Pro
            expanded_extent = self.copy_extent(mapframe, self.base_utils.in_extent)
            expanded_extent = self.buffer_extent(expanded_extent, buffer_percent)
            self.set_map_extent(mapframe, expanded_extent)
        except: # running from Service
            in_extent_split = self.base_utils.in_extent.split(' ')
            if len(in_extent_split) >= 4:
                extent.XMin = in_extent_split[0]
                extent.YMin = in_extent_split[1]
                extent.XMax = in_extent_split[2]
                extent.YMax = in_extent_split[3]

                expanded_extent = mapframe.camera.getExtent()
                expanded_extent = self.buffer_extent(expanded_extent, buffer_percent)
                self.set_map_extent(mapframe, expanded_extent)

    def buffer_extent(self, expanded_extent, buffer_percent):
        extent_buffer_percent = buffer_percent / 100
        xdif = abs(expanded_extent.XMax - expanded_extent.XMin)
        ydif = abs(expanded_extent.YMax - expanded_extent.YMin)
        xmodif = xdif * extent_buffer_percent
        ymodif = ydif * extent_buffer_percent
        expanded_extent.XMin = expanded_extent.XMin - xmodif
        expanded_extent.YMin = expanded_extent.YMin - ymodif
        expanded_extent.XMax = expanded_extent.XMax + xmodif
        expanded_extent.YMax = expanded_extent.YMax + ymodif
        return expanded_extent

    def buffer_extent_by_parts(self, xmin, ymin, xmax, ymax, buffer_percent):
        extent_buffer_percent = buffer_percent / 100
        xdif = abs(xmax - xmin)
        ydif = abs(ymax - ymin)
        xmodif = xdif * extent_buffer_percent
        ymodif = ydif * extent_buffer_percent
        xmin = xmin - xmodif
        ymin = ymin - ymodif
        xmax = xmax + xmodif
        ymax = ymax + ymodif
        return [xmin,ymin,xmax,ymax]

    # def recreate_map_finishing(self, map_obj):
    #     for feature_set in self.base_utils.in_map_finishing_features:
    #         display_field_name = feature_set['displayFieldName']
    #         this_feature_set = copy.deepcopy(feature_set)
    #         cp_feature_set = copy.deepcopy(feature_set)
    #         if 'features' in this_feature_set:
    #             for feature in this_feature_set['features']:
    #                 feature_name = 'feature_{}'.format(self.map_finishing_idx)
    #                 definition = None
    #                 if 'definition' in feature:
    #                     definition = feature['definition']
    #                     del feature['definition']
    #                 cp_feature_set['features'] = [feature]
    #                 layer_obj = self.add_layer_to_map_from_json(cp_feature_set, feature_name, map_obj)
    #                 self.map_finishing_idx += 1
    #                 if definition:
    #                     layer_obj.updateLayerFromJSON(definition)
    #                     if display_field_name == 'label':
    #                         layer_obj.showLabels = True
    #                     else:
    #                         layer_obj.showLabels = False

    ####################
    #### AOI STUFF #####
    ####################

    def set_aoi_extent(self, lyt, aoi_layer, name='map', buffer_percent=10):
        mapframe = self.get_mapframe_from_layout(lyt, name)

        # create buffered aoi extent from aoi layer
        aoi_extent = self.create_aoi_extent(mapframe, aoi_layer, buffer_percent)

        if aoi_extent:
            # set the extent of the map frame
            self.set_map_extent(mapframe, aoi_extent)

    # creates the buffered extent around the aoi
    def create_aoi_extent(self, mapframe, layer, buffer_percent):
        try:
            expanded_extent = mapframe.getLayerExtent(layer)
            return self.buffer_extent(expanded_extent, buffer_percent)
        except (RuntimeError, TypeError, ValueError):
            self.base_utils.add_warning_statement('WARNING: Could not buffer the AOI extent')
            return None

    # Do all of our AOI setup stuff in here
    def aoi_setup(self):
        aoi_layer = self.base_utils.in_aoi

        aoi_layer_name = self.base_utils.config['aoi']['name']
        simplified_aoi_layer_name = self.simplify_layer_name(aoi_layer_name)

        if aoi_layer:
            self.aoi_source = self.get_local_source_by_name(simplified_aoi_layer_name)
            pre_aoi_source = '{}_PRE'.format(self.aoi_source)
           
            try:
                aoi_layer.save(pre_aoi_source)
            except:
                arcpy.CopyFeatures_management(self.base_utils.in_aoi.dataSource, pre_aoi_source)
            sr = arcpy.SpatialReference(102100)
            arcpy.Project_management(pre_aoi_source, self.aoi_source, sr)

    def get_aoi_geometry(self, aoi_layer):
        if 'geometry' not in self.aoi_infos[aoi_layer.name]:
            json_file_url = os.sep.join([self.base_utils.root_dir, 'json', '{}.json'.format(aoi_layer.name)])
            arcpy.FeaturesToJSON_conversion(aoi_layer.dataSource, json_file_url)
            aoi_geometry = {}
            with open(json_file_url) as f:
                aoi_json = json.load(f)
                aoi_geometry['spatialReference'] = aoi_json['spatialReference']
                for feature in aoi_json['features']:
                    if 'rings' in feature['geometry']:
                        feature_rings = feature['geometry']['rings']
                        if 'rings' not in aoi_geometry:
                            aoi_geometry['rings'] = []
                        aoi_geometry['rings'] += feature_rings
                    if 'curveRings' in feature['geometry']:
                        feature_curve_rings = feature['geometry']['curveRings']
                        if 'curveRings' not in aoi_geometry:
                            aoi_geometry['curveRings'] = []
                        aoi_geometry['curveRings'] += feature_curve_rings
            self.aoi_infos[aoi_layer.name]['geometry'] = aoi_geometry
        return self.aoi_infos[aoi_layer.name]['geometry']

    def set_aoi_definition(self, aoi_layer, aoi_geometry, map_obj, lyr_cfg):
        name = lyr_cfg['name']
        clip = lyr_cfg['clip']
        url_source = lyr_cfg['source']
        acres_replace = lyr_cfg['acres_replace']
        percent_of_field_replace = lyr_cfg['percent_of_field_replace']
        visible_fields = lyr_cfg['visible_fields']
        dissolve_field = lyr_cfg['dissolve_field']
        source = self.get_layer_from_map(name, map_obj)

        if source.name not in self.aoi_infos[aoi_layer.name]['definitions']:
            if source.name not in self.original_data_sources:
                self.original_data_sources[source.name] = url_source
            source_url = self.original_data_sources[source.name]

            payload = {
                'f': 'json',
                'outFields': '*',
                'geometry': json.dumps(aoi_geometry),
                'geometryType': 'esriGeometryPolygon',
                'inSR': aoi_geometry['spatialReference']['wkid']
            }
            if self.base_utils.token:
                payload['token'] = self.base_utils.token

            req = requests.post(source_url + '/query', data=payload, verify=False)
            req_json = req.json()
            self.aoi_infos[aoi_layer.name]['definitions'][source.name] = req_json

        filtered_layer = self.add_layer_to_map_from_json(
            self.aoi_infos[aoi_layer.name]['definitions'][source.name],
            source.name,
            map_obj)
        filtered_layer.name = source.name
        if clip:
            clipped_filtered_layer = self.clip_layer(filtered_layer, aoi_layer, map_obj, visible_fields, dissolve_field)
            clipped_layer_name = clipped_filtered_layer.name
            final_clipped_filtered_layer = self.swap_sources_with_template(source, clipped_filtered_layer, map_obj)
            final_clipped_filtered_layer.name = clipped_layer_name
            self.replace_acres_and_percent_of_field(final_clipped_filtered_layer, acres_replace, percent_of_field_replace)
            self.repopulate_unique_values(final_clipped_filtered_layer)
        else:
            self.replace_acres_and_percent_of_field(filtered_layer, acres_replace, percent_of_field_replace)
            self.repopulate_unique_values(filtered_layer)

    def repopulate_unique_values(self, layer):       
        symbology = layer.symbology
        item_groups = symbology.renderer.listMissingValues()
        group_name = item_groups[0].heading
        addition_items = item_groups[0].items
        removal_items = symbology.renderer.groups[0].items
        symbology.renderer.addValues({group_name: addition_items})
        symbology.renderer.removeValues({group_name: removal_items})
        layer.symbology = symbology

    def replace_acres_and_percent_of_field(self, layer, acres_replace, percent_of_field_replace):
        source = self.get_layer_source(layer)
        fields = [acres_replace, percent_of_field_replace, 'SHAPE@']
        total_area = 0
        with arcpy.da.SearchCursor(source, ['SHAPE@']) as cursor:
            for row in cursor:
                total_area += row[0].getArea('GEODESIC', 'ACRES')

        with arcpy.da.UpdateCursor(source, fields) as cursor:
            for row in cursor:
                acres = row[2].getArea('GEODESIC', 'ACRES')
                row[0] = format(acres, '.2f')
                row[1] = format(((acres / total_area)*100), '.2f')
                cursor.updateRow(row)

    def set_all_aoi_definitions(self, aoi_layer, aoi_geometry, lyt_cfg):
        if 'definitions' not in self.aoi_infos[aoi_layer.name]:
            self.aoi_infos[aoi_layer.name]['definitions'] = {}

        map_obj = self.get_map_from_lyt_cfg(lyt_cfg, 'map')

        if 'table_layers' in lyt_cfg:
            table_layers = lyt_cfg['table_layers']
            for layer in table_layers:
                lyr_cfg = table_layers[layer]
                self.set_aoi_definition(aoi_layer, aoi_geometry, map_obj, lyr_cfg)

    def update_aoi_symbology(self, aoi_layer, lyt_cfg=None):
        # change aoi outline color, fill color, and outline size
        aoi_config = self.base_utils.config['aoi']
        outline_color = [255,0,0,100]
        outline_size = 2
        if lyt_cfg is None:
            outline_color = aoi_config['outline_color']
            outline_size = aoi_config['outline_size']
        else:
            outline_color = lyt_cfg['aoi_outline_color']
            outline_size = lyt_cfg['aoi_outline_size']

        sym = aoi_layer.symbology
        sym.renderer.symbol.color = {'RGB': aoi_config['fill_color']}
        sym.renderer.symbol.outlineColor = {'RGB': outline_color}
        sym.renderer.symbol.size = outline_size
        aoi_layer.symbology = sym

    def set_aoi(self, lyt, lyt_cfg):
        # add aoi layer ( or find it in map, if running from Pro )
        map_obj = self.get_map_from_lyt_cfg(lyt_cfg, 'map')
        inset_map_obj = self.get_map_from_lyt_cfg(lyt_cfg, 'inset_map')
        lyt_name = lyt_cfg['name']
        lyt_preserve_extent = lyt_cfg['preserve_extent']
        aoi_layer_name = self.base_utils.config['aoi']['name']
        aoi_layer = self.get_layer_from_map(aoi_layer_name, map_obj)
        inset_aoi_layer = None
        if aoi_layer is None:
            aoi_layer = map_obj.addDataFromPath(self.aoi_source)
            aoi_layer.name = aoi_layer_name
            if inset_map_obj:
                inset_aoi_layer = inset_map_obj.addDataFromPath(self.aoi_source)

        # replace maps
        self.replace_maps(lyt, lyt_cfg)

        # change aoi outline color, fill color, and outline size
        self.update_aoi_symbology(aoi_layer, lyt_cfg)
        if inset_aoi_layer:
            self.update_aoi_symbology(inset_aoi_layer, lyt_cfg)

        # simplify the aoi layer name in order to save to lyrx file ( no spaces allowed )
        simplified_aoi_layer_name = self.simplify_layer_name(aoi_layer.name)
        project_aoi = self.make_lyrx_from_feature(self.aoi_source, simplified_aoi_layer_name, map_obj)
        aoi_layer = self.swap_sources_with_template(aoi_layer, project_aoi, map_obj)

        # add to aoi infos
        if aoi_layer.name not in self.aoi_infos:
            self.aoi_infos[aoi_layer.name] = {}
        
        # swap to local sources
        if 'localize_layers' in lyt_cfg:
            localize_layers = lyt_cfg['localize_layers']
            for localize_layer in localize_layers:
                self.make_local_source(localize_layer, map_obj, localize_layers[localize_layer])

        # set all aoi definitions for this layout, and set extent to aoi area
        aoi_geometry = self.get_aoi_geometry(aoi_layer)
        self.set_all_aoi_definitions(aoi_layer, aoi_geometry, lyt_cfg)

        extent_buffer_percent = None
        if 'extent_buffer_percent' in lyt_cfg:
            extent_buffer_percent = lyt_cfg['extent_buffer_percent']

        if lyt_preserve_extent:
            if extent_buffer_percent:
                self.set_extent_from_input(lyt, 'map', extent_buffer_percent)
            else:
                self.set_extent_from_input(lyt, 'map')

            if inset_aoi_layer:
                self.set_extent_from_input(lyt, 'inset_map', 50)
        else:
            if extent_buffer_percent:
                self.set_aoi_extent(lyt, aoi_layer, 'map', extent_buffer_percent)
            else:
                self.set_aoi_extent(lyt, aoi_layer)

            if inset_aoi_layer:
                self.set_aoi_extent(lyt, inset_aoi_layer, 'inset_map', 50)

        return aoi_layer

    ####################
    ### EXPORT STUFF ###
    ####################

    def export_layouts(self, pdf_doc):
        # get layouts config
        layouts_config = self.base_utils.config['layouts']

        # setup execution percentage for each layout
        execution_percentage = 80
        each_execution_percentage = execution_percentage / len(layouts_config)
        placement_bumper = 0

        for lyt_key in layouts_config:
            lyt_cfg = layouts_config[lyt_key]
            if 'map' in lyt_cfg and lyt_cfg['map'] in self.base_utils.in_map_type:
                lyt_name = lyt_cfg['name']
                placement_bumper = self.export_layout(lyt_name, pdf_doc, lyt_cfg, placement_bumper)
            self.base_utils.increment_execution_percentage(each_execution_percentage)

    def export_layout(self, lyt_name, pdf_doc, lyt_cfg, placement_bumper):
        lyt = self.get_layout_from_aprx(lyt_name)
        aoi_layer = self.set_aoi(lyt, lyt_cfg)

        placement = lyt_cfg['placement']

        map_obj = self.get_map_from_lyt_cfg(lyt_cfg)
        
        # add reference layers and labels
        # self.add_optional_layers(map_obj)

        # replace text components
        self.replace_text(lyt, lyt_cfg)

        # add map finishing components
        # self.recreate_map_finishing(map_obj)

        # if we have a table layer setup, replace the source, and overflow it if necessary
        if 'table_layers' in lyt_cfg:
            table_layers = lyt_cfg['table_layers']
            for layer in table_layers:
                table_layer = table_layers[layer]
                name = table_layer['name']
                clip = table_layer['clip']
                if clip:
                    name = '{} (CLIPPED_DISSOLVED)'.format(name)
                layer = self.get_layer_from_map(name, map_obj)

                max_rows = None
                if 'max_table_rows' in lyt_cfg:
                    max_rows = lyt_cfg['max_table_rows']
                
                sort_field = None
                if 'acres_replace' in table_layer:
                    sort_field = table_layer['acres_replace']
                overflow_clauses = None
                if max_rows:
                    overflow_clauses = self.get_overflow_clauses(layer, aoi_layer, lyt, sort_field, max_rows)
                else:
                    overflow_clauses = self.get_overflow_clauses(layer, aoi_layer, sort_field, lyt)

                # if we have some overflow clauses
                if overflow_clauses:
                    for idx, overflow_clause in enumerate(overflow_clauses): 
                        if idx > 0:
                            placement_bumper += 1

                        url = os.sep.join([self.base_utils.root_dir, 'pdfs', '{0}_{1}.pdf'.format(lyt_cfg['map'], idx)])
                        self.copy_new_project()
                        map_obj = self.get_map_from_lyt_cfg(lyt_cfg)
                        layer = self.get_layer_from_map(name, map_obj)
                        layer.definitionQuery = overflow_clause
                        lyt = self.get_layout_from_aprx(lyt_name)
                        lyt.exportToPDF(url)
                        pdf_doc.insertPages(url, placement + placement_bumper)
                    layer.definitionQuery = '1=1'
        else: # export normally
            url = os.sep.join([self.base_utils.root_dir, 'pdfs', '{}.pdf'.format(lyt_cfg['map'])])
            self.copy_new_project()
            map_obj = self.get_map_from_lyt_cfg(lyt_cfg)
            lyt = self.get_layout_from_aprx(lyt_name)
            lyt.exportToPDF(url)
            pdf_doc.insertPages(url, placement + placement_bumper)
        return placement_bumper

    # def add_optional_layers(self, map_obj):
    #     for layer in self.base_utils.in_optional_layers:
    #         for found_layer in map_obj.listLayers(layer):
    #             found_layer.visible = True

    def get_overflow_clauses(self, layer, aoi_layer, lyt, sort_field=None, max_rows=20):
        try:
            lyt.listElements('MAPSURROUND_ELEMENT', self.base_utils.config['table_frame_element'])[0]
        except (KeyError, IndexError):
            return None

        # get all of the oids that are currently displaying
        oids = self.get_all_oids_for_layer(layer, aoi_layer, sort_field)
        
        # split them up for each page
        sub_oids = []
        for idx, oid in enumerate(oids):
            try:
                sub_oids[math.floor(idx/max_rows)].append(str(oid))
            except IndexError:
                sub_oids.append([])
                sub_oids[math.floor(idx/max_rows)].append(str(oid))
        
        where_clauses = []
        for sub_oid in sub_oids:
            where_clauses.append('objectid IN ({})'.format(','.join(sub_oid)))

        return where_clauses

    # replace maps from the layout config as necessary
    def replace_maps(self, lyt, lyt_cfg):
        # replace maps
        for map_type in self.base_utils.config['map_frames']:
            map_obj = self.get_map_from_lyt_cfg(lyt_cfg, map_type)
            if map_obj:
                map_frame = self.get_mapframe_from_layout(lyt, map_type)
                map_frame.map = map_obj

    # replace text from the layout config as necessary
    def replace_text(self, lyt, lyt_cfg):
        # replace text

        # first look from this layout's specific replacement config
        replacement_cfg = lyt_cfg['replacement']
        for key in replacement_cfg:
            replacement_json = replacement_cfg[key]
            replacement_value = self.parse_replacement_json(replacement_json)

            for element in lyt.listElements('TEXT_ELEMENT'):
                finder = '@@@{}@@@'.format(key)
                if finder in element.text:
                    element.text = element.text.replace(finder, replacement_value)

        # then look from the generic replacement config
        generic_replacement_cfg = self.base_utils.config['generic_replacement']
        for key in generic_replacement_cfg:
            replacement_json = generic_replacement_cfg[key]
            if 'value' not in replacement_json: 
                # if we haven't made this generic replacement before,
                # make the request or calculate the text
                replacement_json['value'] = self.parse_replacement_json(replacement_json)
                self.base_utils.config['generic_replacement'][key] = replacement_json
            replacement_value = replacement_json['value']
            
            for element in lyt.listElements('TEXT_ELEMENT'):
                finder = '@@@{}@@@'.format(key)
                if finder in element.text:
                    element.text = element.text.replace(finder, replacement_value)

    # parse our replacement json from the config
    def parse_replacement_json(self, replacement_json):
        replacement = ''
        if 'type' in replacement_json:
            replacement_type = self.try_read_replacement_key(replacement_json, 'type')
            appender = self.try_read_replacement_key(replacement_json, 'append')
            prepender = self.try_read_replacement_key(replacement_json, 'prepend')
            if replacement_type == 'text':
                replacement = self.try_read_replacement_key(replacement_json, 'value')
            elif replacement_type in ['query', 'identify', 'computeStatisticsHistograms']:
                replacement = self.make_replacement_request(replacement_json)
            elif replacement_type == 'calculate':
                replacement =self.calculate_replacement_geometry(replacement_json)
            return '{0}{1}{2}'.format(prepender, replacement, appender)
        return replacement

    def calculate_centroid(self, source):
        with arcpy.da.SearchCursor(source, ['SHAPE@']) as cursor:
            for row in cursor:
                point = row[0].centroid
                return '{0},{1}'.format(point.X, point.Y)
        return None

    def calculate_replacement_geometry(self, replacement_json):
        source = self.get_local_source_by_name(replacement_json['layer'])
        calculate_property = self.try_read_replacement_key(replacement_json, 'property')
        area_unit = self.try_read_replacement_key(replacement_json, 'area_unit')

        desc = arcpy.Describe(source)
        with arcpy.da.SearchCursor(source, ['SHAPE@']) as cursor:
            for row in cursor:
                if calculate_property == 'area' and desc.shapeType == 'Polygon':
                    return str(row[0].getArea(units=area_unit))
                else:
                    return str(0)
        return ''

    def build_replacement_payload(self, replacement_json):
        payload = {}
        req_type = self.try_read_replacement_key(replacement_json, 'type')
        if req_type == 'computeStatisticsHistograms':
            payload = {
                'f': 'json',
            }
            if 'pixel_size' in replacement_json:
                payload['pixelSize'] = '{0},{0}'.format(replacement_json['pixel_size'])
            if 'rendering_rule' in replacement_json:
                payload['renderingRule'] = json.dumps(replacement_json['rendering_rule'])
            if 'in_aoi' in replacement_json and replacement_json['in_aoi']:
                payload['geometry'] = json.dumps(self.aoi_infos[replacement_json['in_aoi']]['geometry'])
                payload['geometryType'] = 'esriGeometryPolygon'
        elif req_type == 'query':
            payload = {
                'f': 'json',
                'returnGeometry': False,
                'returnDistinctValues': True
            }
            if 'where' in replacement_json:
                payload['where'] = replacement_json['where']
            if 'return_field' in replacement_json:
                payload['outFields'] = replacement_json['return_field']
            if 'in_aoi' in replacement_json:
                if 'use_centroid' in replacement_json and replacement_json['use_centroid']:
                    centroid = self.calculate_centroid(self.aoi_source)
                    if centroid:
                        payload['geometry'] = centroid
                        payload['geometryType'] = 'esriGeometryPoint'
                        payload['inSR'] = self.aoi_infos[replacement_json['in_aoi']]['geometry']['spatialReference']['wkid']
                else:
                    payload['geometry'] = json.dumps(self.aoi_infos[replacement_json['in_aoi']]['geometry'])
                    payload['geometryType'] = 'esriGeometryPolygon'
                    payload['inSR'] = self.aoi_infos[replacement_json['in_aoi']]['geometry']['spatialReference']['wkid']
        if self.base_utils.token:
            payload['token'] = self.base_utils.token
        return payload

    def try_read_replacement_key(self, replacement_json, key):
        try:
            return replacement_json[key]
        except KeyError:
            return ''

    def parse_replacement_response(self, response, replacement_json):
        req_type = self.try_read_replacement_key(replacement_json, 'type')
        rep_calc = self.try_read_replacement_key(replacement_json, 'calc')
        rep_units = self.try_read_replacement_key(replacement_json, 'units')
        rep_conversion_coefficient = self.try_read_replacement_key(replacement_json, 'conversion_coefficient')
        if req_type == 'query' and 'features' in response:
            features = response['features']
            if len(features) > 1:
                return replacement_json['multiple_value_placeholder'] # if we get multiple values
            elif len(features) < 1:
                return '' # if we get no values
            else:
                return features[0]['attributes'][replacement_json['return_field']]
        elif req_type == 'computeStatisticsHistograms' and 'statistics' in response:
            try:
                calc_value = response['statistics'][0][rep_calc]
                if rep_conversion_coefficient and rep_conversion_coefficient != '':
                    calc_value = calc_value * rep_conversion_coefficient
                return '{0:.2f} {1}'.format(calc_value, rep_units)
            except KeyError:
                return ''
        return ''

    # make the replacement request to return a string for replacement
    def make_replacement_request(self, replacement_json):
        payload = self.build_replacement_payload(replacement_json)
        req_type = self.try_read_replacement_key(replacement_json, 'type')
        pixel_size = None
        if 'pixel_size' in replacement_json:
            pixel_size = replacement_json['pixel_size']
        if 'service' in replacement_json:
            req = requests.post('{0}/{1}'.format(replacement_json['service'], req_type), data=payload, verify=False)
            req_json = req.json()
            if 'error' in req_json and req_json['error']['code'] == 498:
                # if we have an invalid token, try again without a token
                del payload['token']
                req = requests.post('{0}/{1}'.format(replacement_json['service'], req_type), data=payload, verify=False)
                req_json = req.json()
            while 'error' in req_json and req_json['error']['code'] == 400 and pixel_size:
                # image exceeded size limit
                pixel_size *= 2
                payload['pixelSize'] = '{0},{0}'.format(pixel_size)
                req = requests.post('{0}/{1}'.format(replacement_json['service'], req_type), data=payload, verify=False)
                req_json = req.json()
            return self.parse_replacement_response(req_json, replacement_json)
        return ''