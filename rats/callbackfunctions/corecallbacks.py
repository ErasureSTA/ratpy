from dash.dependencies import Input, Output, State
from rats.core.app import app
import dash_html_components as html
from dash.exceptions import PreventUpdate
import pandas as pd
import plotly_express as px
from rats.modules import ratparser
from flask import request
import pathlib
import platform
import dash_table
import dash_uploader as du
import os
import shutil
import base64
from bs4 import BeautifulSoup as bs

if platform.system() == 'Windows':
    cachepath = '\\cache\\'
    dfpath = '\\feathereddataframes\\'
    figurepath = '\\pickledfigures\\'
    topopath = '\\topo\\'
else:
    cachepath = '/cache/'
    dfpath = '/feathereddataframes/'
    topopath = '/topo/'

packagepath = pathlib.Path(__file__).parent.parent.resolve()
du.configure_upload(app, str(packagepath) + cachepath, use_upload_id=False, )

'''
Contents: 
1 - Dropdown population 
    - Generic placeholder generation, fired on server startup
2 - File upload 
    - No real purpose right now as upload handled by dash_uploader module 
3 - Data pre-processing 
    - preprocessdata()
        - Creates dataframes, list of files in session and performs comparison between dataframes
        - compares dataframes and checks for intra and inter file errors
4 - Figure management
    - clearprogramdata()
        - clears out files from rats/feathereddataframes and rats/pickledfigures
5 - Server shutdown 
    - shutdown()
        - clears the RATS files and sessionfilenames dataframe from the rats/cache directory

'''

# ============================================================
#           DROPDOWN POPULATION
# ============================================================
filenames = []
dataframes = []
bigpictureplotdata = []
scopeplotdata = []
interscanplotdata = []

dropdownoptions = []
for i in range(12):
    item = {'label': f'Data Slot {i + 1}', 'value': i}
    dropdownoptions.append(item)

placeholderfig = px.line(x=[1, 2, 3, 4], y=[1, 2, 3, 4],
                         title='placeholder')  # will be changed later, possibly to something representative

colors = {'background': '#111111', 'text': '#7FDBFF'}

dropdownoptions = []
for i in range(12):
    item = {'label': f'Data Slot {i + 1}', 'value': i}
    dropdownoptions.append(item)


# ============================================================
#           /DROPDOWN POPULATION
# ============================================================

# ============================================================
#           FILE UPLOAD
# ============================================================
# DATAFRAME PRODUCTION
@app.callback(Output('errorlog', 'children'),
              Input('dash-uploader', 'isCompleted'),
              State('errorlog', 'children'))
def upload(complete, error):
    # format the relevant information into a dataframe, append later to the session dataframe
    # this returns after pre-processing and it's not clear why, so it preserves the sate of the error log
    if not complete:
        return
    return error


# ============================================================
#           /FILE UPLOAD
# ============================================================

# ============================================================
#           DATA PRE-PROCESSING
# ============================================================

@app.callback([Output('datalist', 'children'),
               Output('errorlogcontainer', 'children'),
               Output('ratdashpullcontainer', 'children'),
               Output('scopepullcontainer', 'children'),
               Output('interscanpullcontainer', 'children')],
              [Input('processdata', 'n_clicks')])
def preprocessdata(click):
    errormessage = ''
    # function to add to the session file dataframe the status of each file and yield the gui output
    if click is None:
        raise PreventUpdate

    list_of_names = []
    for filename in os.listdir(str(packagepath / 'cache')):
        if '.txt' in filename:
            list_of_names.append(filename)

    filenamedf = pd.DataFrame(dict(file=list_of_names, processed=['no'] * len(list_of_names)))

    for i in list_of_names:
        try:
            # try to load an existing sessionfilenames
            sessionfilenames = pd.read_feather(str(packagepath) + cachepath + 'sessionfilenames')
            if i not in sessionfilenames['file'].unique():
                # add this new file to the sessionfilenames dataframe if it's not already there
                sessionfilenames = sessionfilenames.append(filenamedf, ignore_index=True)
                sessionfilenames.to_feather(str(packagepath) + cachepath + 'sessionfilenames')
        except Exception:
            # if there was no cache of session filenames, then use this filename to create the dataframe in the cache
            sessionfilenames = filenamedf
            sessionfilenames.to_feather(str(packagepath) + cachepath + 'sessionfilenames')

        try:
            # see if there's a valid dataframe stored for this file
            pd.read_feather(str(packagepath) + dfpath + f'{i}.feather')
            print('found df')
        except Exception:
            # should be a case of now reading this file in from cache instead of from some absolute path...
            parser = ratparser.RatParse(str(packagepath) + cachepath + f'{i}')
            # soomething's going wrong here when reading file back in from cache
            if parser.verified:
                df = parser.dataframe
                df.to_feather(str(packagepath) + dfpath + f'{i}.feather')
            else:
                # remove the filename from the session and the file from the cache
                sessionfilenames = pd.read_feather(str(packagepath) + cachepath + 'sessionfilenames')
                sessionfilenames = sessionfilenames[sessionfilenames['file'] != i]
                sessionfilenames.reset_index(drop=True, inplace=True)
                sessionfilenames.to_feather(str(packagepath) + cachepath + 'sessionfilenames')
                file_path = os.path.join(str(packagepath / 'cache'), i)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))
                errormessage = f'File {i} could not be verified as a RATS file'

    try:
        sessionfilenames = sessionfilenames.drop_duplicates(subset='file')
        sessionfilenames.reset_index(drop=True, inplace=True)
        filenames = sessionfilenames['file'].tolist()
        log = []
        for i in filenames:
            message = ''
            df1 = pd.read_feather(str(packagepath) + dfpath + f'{i}.feather')
            if 1 in df1['anomalous'].tolist():
                message += 'There may be an error in this file\n '
            df1 = df1.drop_duplicates(subset=['llc'])
            for j in filenames:
                if j != i:
                    df2 = pd.read_feather(str(packagepath) + dfpath + f'{j}.feather')
                    df2 = df2.drop_duplicates(subset=['llc'])
                    if df1['function'].equals(df2['function']):
                        print(f'File {j} complements file {i}')
                    else:
                        print(f'File {j} does not complement file {i}')
                        message += f'This file does not complement file {j}\n '
            if message == '':
                log.append('No issues detected\n')
            else:
                log.append(message)

        sessionfilenames['log'] = log
        sessionfilenames['processed'] = 'yes'
        sessionfilenames.to_feather(str(packagepath) + cachepath + 'sessionfilenames')
        print('stored sessionfilenemes df')

        tabledata = sessionfilenames[['file', 'log']]
        children = dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in tabledata.columns],
            data=tabledata.to_dict('records'),
            style_data_conditional=[{
                'if': {
                    'filter_query': "{log} contains 'There may be an error in this file'",
                    'column_id': 'log'

                },
                'border-top-color': 'Tomato',
                'border-bottom-color': 'Tomato',
                'border-top-style': 'solid',
                'border-bottom-style': 'solid',
                'border-top-width': '1px',
                'border-bottom-width': '1px',
                'background-color': 'rgba(255,65,54,0.2)'

            },
                {
                    'if': {
                        'filter_query': "{log} contains 'This file does not complement file'",
                        'column_id': 'log'

                    },
                    'border-top-color': 'Tomato',
                    'border-bottom-color': 'Tomato',
                    'border-top-style': 'solid',
                    'border-bottom-style': 'solid',
                    'border-top-width': '1px',
                    'border-bottom-width': '1px',
                    'background-color': 'rgba(252,186,3,0.2)'
                },
                {
                    'if': {
                        'filter_query': "{log} contains 'No issues'",
                        'column_id': 'log'

                    },
                    'border-top-color': 'Green',
                    'border-bottom-color': 'Green',
                    'border-top-style': 'solid',
                    'border-bottom-style': 'solid',
                    'border-top-width': '1px',
                    'border-bottom-width': '1px',
                    'background-color': 'rgba(60,201,72,0.2)'
                },
            ],
            style_cell={'whiteSpace': 'pre-line',
                        'textAlign': 'center',
                        'font-family': 'sans-serif'},
            style_as_list_view=True)
        # all manipulation of sessionfilenames dataframe is now complete...
        print(errormessage)

        rdbutton = html.Button(id='pulldataratdash', children='Pull the data into Big Picture app',
                               className='btn btn-secondary', type='button')
        sbutton = html.Button(id='pulldatascope', children='Pull the data into Scope app',
                              className='btn btn-secondary', type='button')
        ibutton = html.Button(id='pulldatainterscan', children='Pull the data into Interscan app',
                              className='btn btn-secondary', type='button')

        return children, html.Div([errormessage], id='errorlog', className='col'), rdbutton, sbutton, ibutton
    except Exception:
        print('epic fail')
        rdbutton = html.Button(id='pulldataratdash', children='Pull the data into Big Picture app',
                               className='btn btn-secondary', type='button')
        sbutton = html.Button(id='pulldatascope', children='Pull the data into Scope app',
                              className='btn btn-secondary', type='button')
        ibutton = html.Button(id='pulldatainterscan', children='Pull the data into Interscan app',
                              className='btn btn-secondary', type='button')
        return [], 'failed to preprocess data', rdbutton, sbutton, ibutton
        pass


# ============================================================
#           /DATA PRE-PROCESSING
# ============================================================


# ============================================================
#           CLEAR PROGRAM DATA
# ============================================================
@app.callback(Output('clearstatus', 'children'),
              [Input('cleardata', 'n_clicks')])
def clearprogramdata(n_clicks):
    if n_clicks is None:
        pass
    else:
        print('Ratdash has cleared all the program data!')

        # clear session data before shutdown
        for filename in os.listdir(str(packagepath / 'feathereddataframes')):
            if filename != '__init__.py':
                file_path = os.path.join(str(packagepath / 'feathereddataframes'), filename)
            else:
                file_path = False
            if file_path:
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))

        return 'All previously processed data has been cleared'


# ============================================================
#           /CLEAR PROOGRAM DATA
# ============================================================

# ============================================================
#           PROGRAM SHUTDOWN
# ============================================================
@app.callback(Output('runstatus', 'children'),
              [Input('shutdown', 'n_clicks')])
def shutdown(n_clicks):
    if n_clicks is None:
        pass
    else:
        print('Ratdash says goodbye!')

        for filename in os.listdir(str(packagepath / 'cache')):
            if filename != '__init__.py':
                file_path = os.path.join(str(packagepath / 'cache'), filename)
            else:
                file_path = False
            if file_path:
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))

        func = request.environ.get('werkzeug.server.shutdown')
        func()
        return 'Server has been shut down, please close the browser window'


# ============================================================
#           /PROGRAM SHUTDOWN
# ============================================================


# ============================================================
#           TOPO MANAGEMENT
# ============================================================

def populatetoporeport():
    """
    Finds loaded topo files and constructs a DF for display with dash_table of the files and the device to which they
    map.
    :return: dash_table
    """
    topodict = []
    for filename in os.listdir(str(packagepath) + topopath):
        print(filename)
        if '.xml' in filename:
            topology = {}
            if filename == '__init__.py':
                pass
            elif 'NETWORK' in filename:
                topology['filename'] = filename
                topology['device'] = 'Instrument Topo File'
                topodict.append(topology)
            else:
                with open(str(packagepath) + topopath + filename, 'r') as f:
                    content = f.readlines()
                content = "".join(content)
                soup = bs(content, 'lxml')
                device = soup.find('de:device')
                topology['filename'] = filename
                topology['device'] = f'EDS for {device["instancename"]}'
                topodict.append(topology)
        topodf = pd.DataFrame(topodict)
        children = dash_table.DataTable(
            id='topotable',
            columns=[{"name": i, "id": i} for i in topodf.columns],
            data=topodf.to_dict('records'))
    return children


def parse_topo(contents, filename):
    """
    Handle uploads from dcc.upload component - topo files are small enough for this to be effective.
    """
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    print(content_type)
    try:
        if '.xml' in filename:
            print(filename)
            print('xml found')
            print(str(decoded[2:]))
            with open(str(packagepath) + topopath + filename, 'w') as f:
                f.write(str(decoded))
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])


@app.callback(Output('toporeport', 'children'),
              Input('upload-topo', 'contents'),
              State('upload-topo', 'filename'))
def update_toporeport(list_of_contents, list_of_names):
    """
    Upload topography files and update the displayed output
    """
    if list_of_contents is None:
        raise PreventUpdate
    elif list_of_contents is not None:
        [parse_topo(c, n) for c, n in zip(list_of_contents, list_of_names)]
        children = populatetoporeport()
        return children


# function to delete what's in the topo folder required

@app.callback([Output('clearedtopo', 'children'),
               Output('topodatacontainer', 'children')],
              [Input('cleartopo', 'n_clicks')])
def cleartopodata(n_clicks):
    """
    clear all topo data
    """
    if n_clicks is None:
        pass
    else:
        # clear session data before shutdown
        for filename in os.listdir(str(packagepath) + topopath):
            if filename != '__init__.py':
                file_path = os.path.join(str(packagepath) + topopath + filename)
            else:
                file_path = False
            if file_path:
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                    print('topo data deleted')
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))
        return 'All topo data has been cleared', html.Div([], id='toporeport', className='col')
