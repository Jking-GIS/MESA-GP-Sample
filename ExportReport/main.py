# Developed by Jeffrey King @ Esri
# October 2019

# AUTO REPLACED GLOBAL VARS
# ---

root_dir = 'C:\\Users\\jeff9123\\Documents\\github\\MESA-GP\\ExportReport'
resources_dir = 'C:\\Users\\jeff9123\\Documents\\github\\MESA-GP\\ExportReport\\resources'
utils_dir = 'C:\\Users\\jeff9123\\Documents\\github\\MESA-GP\\ExportReport\\utils'

def try_import():
    global BaseUtils, AprxUtils
    from utils import baseUtils
    from utils import aprxUtils
    BaseUtils = baseUtils.BaseUtils
    AprxUtils = aprxUtils.AprxUtils

import sys, os, arcpy

try:
    try_import()
except:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
    try_import()

# the main function
def main():
    base_utils = BaseUtils(root_dir, resources_dir)
    aprx_utils = AprxUtils(base_utils)

    # Do some setup
    base_utils.create_SSL_context()
    if base_utils.sign_into_portal() is None:
        base_utils.add_warning_statement('Could not sign into portal, some layers may not be added')

    # Open original base aprx
    aprx_utils.open_aprx()

    # initialize local dynamic path
    base_utils.initialize_paths()

    # Save project to a new local copy, in dynamically created dir
    aprx_utils.copy_new_project()

    # 5%
    base_utils.increment_execution_percentage(5)

    # setup the aoi geometry and add to map
    aprx_utils.aoi_setup()

    # create pdf doc
    full_report_url = os.sep.join([base_utils.output_dir, '{}.pdf'.format(base_utils.config['report_filename'])])
    pdf_doc = arcpy.mp.PDFDocumentCreate(full_report_url)
    
    # 15%
    base_utils.increment_execution_percentage(10)

    # export all the necessary layouts
    aprx_utils.export_layouts(pdf_doc)

    # save the temp pdf
    pdf_doc.saveAndClose() 

    # save at the end
    aprx_utils.aprx.save()

    # return the external report url
    external_report_url = base_utils.get_external_report_url(full_report_url)
    
    # 97%
    base_utils.increment_execution_percentage(2)

    # final cleanup
    base_utils.clean_folder()

    # 100%
    base_utils.increment_execution_percentage(3)

    # set the external report url to return to user
    base_utils.set_output(external_report_url)

# main
if __name__ == '__main__':
    main()