import arcpy
import os
import datetime
import ssl
import re
import json
import sys
import shutil
import smtplib
import requests

from operator import itemgetter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

arcpy.env.overwriteOutput = True

class BaseUtils(object):
    def __init__(self, root_dir, resources_dir):
        self.in_aoi = arcpy.GetParameter(0)
        self.in_extent = arcpy.GetParameter(1)
        self.in_map_type = arcpy.GetParameter(2)

        self.warning_statements = []
        self.root_dir = root_dir
        self.resources_dir = resources_dir
        self.original_root_dir = root_dir
        self.output_dir = arcpy.env.scratchWorkspace

        self.cur_time = '{0:%Y%m%d_%H%M%S}'.format(datetime.datetime.now())
        self.cur_date = '{0:%Y%m%d}'.format(datetime.datetime.now())

        self.execution_percentage = 0

        self.page_cnt = 0

        self.token = None

        # look at root directory and import the config
        try:
            self.report_folder_name = re.split(r'/|\\', self.root_dir)[-1]
            with open(os.sep.join([self.resources_dir, 'config.json'])) as f:
                self.config = json.load(f)
        except FileNotFoundError:
            sys.exit()
    
    def set_output(self, report_url):
        arcpy.SetParameter(3, report_url)

    def add_warning_statement(self, warning):
        arcpy.AddWarning(warning)
        self.warning_statements.append(warning)

    # increment the execution percentage for loading bar in web app
    def increment_execution_percentage(self, percent):
        self.execution_percentage += percent
        arcpy.AddMessage('EXECUTION: {0:.2f}%'.format(self.execution_percentage))

    # initialize our dynamic paths
    def initialize_paths(self):
        try:
            # Make sure path exists for exporting
            reports_path = os.sep.join([self.root_dir, 'reports'])
            if not os.path.exists(reports_path):
                os.makedirs(reports_path)
            path = os.sep.join([reports_path, self.cur_time])
            if not os.path.exists(path):
                os.makedirs(path)
                os.makedirs(os.sep.join([path, 'pdfs']))
                os.makedirs(os.sep.join([path, 'lyrx']))
                os.makedirs(os.sep.join([path, 'json']))
                self.root_dir = path
                arcpy.management.CreateFileGDB(self.root_dir, '{}.gdb'.format(self.config['gdb_name']))
            self.check_output_directory()
        except (RuntimeError, TypeError, ValueError):
            self.add_warning_statement('WARNING: Could not initialize output folder for report')

    def check_output_directory(self):
        if not self.output_dir:
            self.output_dir = self.root_dir
        else:
            path_splitter = re.split(r'/|\\', self.output_dir)
            last_path_folder = path_splitter[-1]
            split_for_gdb = last_path_folder.split('.')
            is_gdb = last_path_folder.split('.')[-1] == 'gdb'
            if is_gdb:
                self.output_dir = self.root_dir

    # Clean up folder to only hold the last X reports created
    # (where X is max_report_buffer in config.json)
    def clean_folder(self):
        clean_dates = []
        reports_directory = os.sep.join([self.original_root_dir, 'reports'])
        for file in os.listdir(reports_directory):
            try:
                if file.find('\\') < 0 and file.find('/') < 0 and file.find('.') < 0:
                    full_dir = os.sep.join([reports_directory, file])
                    clean_dates.append([os.stat(full_dir).st_mtime, full_dir])
            except:
                pass

        sorted(clean_dates, key=itemgetter(0))
        if len(clean_dates) > self.config['max_report_buffer']:
            for i in range(0, (len(clean_dates)-self.config['max_report_buffer'])):
                folder_to_remove = clean_dates[i][1]
                try:
                    shutil.rmtree(folder_to_remove)
                except PermissionError:
                    self.add_warning_statement(folder_to_remove + ': this folder is currently being used and could not be deleted')

    def get_external_report_url(self, full_report_url):
        ags_path = os.sep.join(re.split(r'/|\\', self.output_dir)[0:2])
        return full_report_url.replace(ags_path, os.sep.join([self.config['external_url'], 'rest'])).replace('\\', '/')

    # create a default ssl context to correctly authorize internal urls
    def create_SSL_context(self):
        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            # Legacy Python that doesn't verify HTTPS certificates by default
            pass
        else:
            # Handle target environment that doesn't support HTTPS verification
            ssl._create_default_https_context = _create_unverified_https_context

    def sign_into_portal(self):
        portal_config = self.config['portal']
        portal_url = arcpy.GetActivePortalURL()
        if 'username' in portal_config and 'password' in portal_config:
            try:
                portal_info = arcpy.SignInToPortal(
                    portal_url,
                    portal_config['username'],
                    portal_config['password']
                )
                token = portal_info['token']
            except (ValueError, KeyError):
                return None
        elif 'app_id' in portal_config and 'refresh_token' in portal_config:
            try:
                payload = {
                    'client_id': portal_config['app_id'],
                    'refresh_token': portal_config['refresh_token'],
                    'grant_type': 'refresh_token'
                }

                req = requests.post(portal_url + '/sharing/rest/oauth2/token', data=payload, verify=False)
                req_json = req.json()
                token = req_json['access_token']
            except (ValueError, KeyError):
                return None
        elif 'app_id' in portal_config and 'app_secret' in portal_config:
            try:
                payload = {
                    'client_id': portal_config['app_id'],
                    'client_secret': portal_config['app_secret'],
                    'grant_type': 'client_credentials'
                }

                req = requests.post(portal_url + '/sharing/rest/oauth2/token', data=payload, verify=False)
                req_json = req.json()
                token = req_json['access_token']
            except (ValueError, KeyError):
                return None
        else:
            infos = arcpy.GetSigninToken()
            if infos:
                    token = infos['token']
            else:
                return None

        self.token = token
        return self.token

    def reset_page_cnt(self):
        self.page_cnt = 0
        self.bookmark_tracker = []

    def increment_page_cnt(self, layer_name=None):
        self.page_cnt += 1
        if layer_name:
            self.all_content_pages.append(layer_name)
            self.bookmark_tracker.append(layer_name)
        return self.page_cnt