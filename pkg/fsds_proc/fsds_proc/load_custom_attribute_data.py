'''
Helper functions for loading attributes data. Starting with CAMELS 
    and GAGES-II attributes.
author:: Benjamin Choat <benjamin.choat@noaa.gov>
description:: given usgs catchment ID's, attributes are read in for 
    those ID's. 
notes:: developed using python v3.11.8; created 2024/08/02
'''

# TODO: 
# - handle case where only one gage is provided; should only need to edit xcl sect.
# - in excl sect. check gage_id is in tab
# - if list of gage_ids provided, then first subset dfs before merging

# import libraries needed across all functions
import pandas as pd
from glob import glob
import pandas as pd
from pathlib import Path
import yaml
import xarray as xr
import netCDF4
import warnings
import os
import shutil
from importlib import resources as impresources
from fsds_proc import data
from itertools import compress
from proc_eval_metrics import read_schm_ls_of_dict
from openpyxl import load_workbook


def process_custom_attributes(config_file: str | os.PathLike) -> pd.DataFrame:
    '''
    Given information provided in custom_attr_config.yaml, loads
    in custom attributes, standardizes them, and saves them to
    destination as file type specified in custom_attr_config.yaml.

    Inputs
    -------------
    path (str or path): path to folder holding CAMELS data.
    ids (list): list of gauge-ids associated with CAMELS data and 
        to be extracted when this function is executed.
        Be careful that leading 0's are present in ids.

    Outputs
    --------------
    df (pandas DataFrame): 
    
    '''

    # load YAML config file
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    # process configuration file    
    
    # get list of files
    files_in = list(config['files_in'].keys())
    # check if tabs listed, if so treat tabs as files
    tab_files = [x for x in files_in if 'tabs' in config['files_in'][x]]
    # check if any non-excel files contain 'tabs', they should not
    tab_nonxlsx = [x for x in tab_files if \
                    x.split('.')[len(x.split('.'))-1] not in ['xlsx', 'xlsm']]
    if len(tab_nonxlsx) > 0:
        raise ValueError(
                    "In configuration file, 'tabs' should only be included for\n"
                    f"   .xlsx or .xlsm files but you've included 'tabs' for {tab_nonxlsx}"
                )
    # get count of files+tabs to ensure inputs for gage_ids match
    file_tab_count = len(files_in)
    for tf in tab_files:
        # get count of tabs for file listed in config file
        file_tab_count += len(config['files_in'][tf]['tabs'])
    # account for xlsx/xlsm files being counted
    file_tab_count -= len(tab_files)


    # get key, value pairs from column schema
    kv_colschema = [
         (k, v) for item in config['col_schema'] for k, v in item.items()
         ]
    # loop through list and handle according to key
    # for item in config['col_schema']:
    for key, value in kv_colschema:
        if key == 'gage_ids':
            if isinstance(value, list):
                if len(value) > 1:

                    # ensure correct number of gage id columns if multiple provided
                    assert len(value) == file_tab_count, \
                        (f"You provided {len(value)} values for gage_ids but "
                         f"listed {file_tab_count} files and/or tabs.\n" 
                         "  When providing multiple id columns for gage_ids in\n"
                        "   the configuration yaml file, there must be one for\n"
                        "   each file and tab. If all files and tabs have the same\n"
                        "   gage ID column name, then provide a single value.")
                    
                gage_id_cols = value

            elif isinstance(value, str):
                gage_id_cols = [value]

            else:
                raise ValueError(
                    "gage_ids must be a single string or a list of stings."
                )
            print(f'gage cols: {gage_id_cols}')

        else:
            print('WILL NEED TO EDIT IF ADD ATTRS TO col_schema')

    # define empty dict to append data to as read in
    dict_dfs_in = {f: [] for f in files_in}

    # j is indexer that accounts for progress in gage_id_cols 
    # resulting from excel workbook tabs. It modifies i to get 
    # correct gage_id column name.
    j = 0

    # process files listed in files_in
    for i, file in enumerate(files_in):
        
        print(f'Processing file {i+1} of {len(files_in)}\nfile: {file}')
        # store config for file in dict_temp
        dict_temp = config['files_in'][file]

        # get gage_id column name so it can be read in as string in case
        # there are leading 0's
        gage_id_temp = gage_id_cols[i+j] if len(gage_id_cols)>1 \
                            else gage_id_cols[0]

        # get extension to indicate file type
        temp = file.split('.')
        extension = temp[len(temp)-1]

        # read in txt, csv, 
        if extension in ['txt', 'csv']:
            if 'separator' in dict_temp and len(dict_temp['separator']) == 1:
                separator = dict_temp['separator'][0]

                # read in file
                try:
                    df_temp = pd.read_csv(
                        file, 
                        sep=separator,
                        dtype={gage_id_temp: str},
                        usecols=[gage_id_temp] + dict_temp['columns']
                        )
                except:
                    raise
            else:
                warnings.warn(
                    f"separator was not provided for \n{file} in config "
                    " yaml file. \n    so attempting to identify automatically."
                )

                # None uses python csv sniffer to id separator
                try:
                    df_temp = pd.read_csv(
                        file, 
                        sep=None, 
                        engine='python',
                        dtype={gage_id_temp: str},
                        usecols=[gage_id_temp] + dict_temp['columns']
                        )
                except:
                    raise

            # check that listed columns match current file columns
            # id columns listed in config yaml if they are not in current file
            missing_config_cols = [
                x for x in dict_temp['columns'] if x not in df_temp.columns
            ]
            
            # if there are missing columns, return error
            if len(missing_config_cols) > 0:
                raise ValueError(
                    "In the configuration yaml file the following\n"
                    f"  columns were listed, but they do not appear in\n"
                    f"  {file}:\n{missing_config_cols}"
                )
            
            # check specified gage_id column is present in current file
            if (len(gage_id_cols) == 1 and 
                    gage_id_temp not in df_temp.columns):
                raise ValueError(
                    f"In the configuration file '{gage_id_cols[0]}'\n"
                    "   was specified to be the name of the\n"
                    "   id column, but there does not\n"
                    f"  seem to be a column with that name in {file}."
                )
            
            # if specified gage_id is not present check other listed col names
            elif (len(gage_id_cols) > 1 and 
                    gage_id_temp not in df_temp.columns):

                # id other listed gage_ids that appear in current file
                # ids_present = [x for x in gage_id_cols if x in df_temp.columns]               
                raise ValueError(
                    f"In the configuration file '{gage_id_temp}'\n"
                    "   was specified to be the name of the\n"
                    "   id column, but there does not\n"
                    f"   seem to be a column with that name in {file}.\n"
                )
            else:
                print(f'specified gage_id column is present in {file}.')

        elif extension in ['xlsx', 'xlsm']:

            # load excel workbook
            try:
                excel_wb = load_workbook(
                                filename=file,
                                data_only = True,
                                read_only=True
                                )
            except:
                raise
            # check if current file included tabs; if not then assume first tab is desired
            if file in tab_files:

                # get list of tabs associated with current file
                tabs_temp = list(dict_temp['tabs'].keys())

                # create dict to hold gage_id_column for each tab-used for mergin in end
                dict_gg_temp = {x: [] for x in tabs_temp}

                # create empty list to store data from tabs in
                dict_dfs_tabs = {tab: [] for tab in tabs_temp}
                for tab in tabs_temp:
    BEN WORK HERE!!!!!! - check that gageid column name is in df_tab
                    if len(gage_id_cols) == 1:
                        gage_id_temp = gage_id_cols[0]
                    else:
                        gage_id_temp = gage_id_cols[i+j]

                    # extract worksheet and convert to df generator
                    excel_ws = excel_wb[tab].values
                    # store first row as colnames
                    columns = next(excel_ws)

                    # create dataframe
                    df_tab = pd.DataFrame(excel_ws, columns=columns)

                    # subset to desired columns
                    cols_keep = [gage_id_temp] + dict_temp['tabs'][tab]['columns']

                    # add current dataframe to tabs output dict
                    dict_dfs_tabs[tab] = df_tab[cols_keep]

                    # add gage_id to dict holding gage_id for each tab
                    dict_gg_temp[tab] = gage_id_temp

                    # update j to account for progress through gage_id_cols
                    j += 1

                # merge dataframes using gage_ids
                df_temp = dict_dfs_tabs[tabs_temp[0]]
                for tab in tabs_temp[1:]:
                    df_temp = pd.merge(
                        df_temp, dict_dfs_tabs[tab],
                        left_on = dict_gg_temp[tabs_temp[0]],
                        right_on = dict_gg_temp[tab],
                        how = 'inner'
                    )
            # if xlsx or xlsm and tab not included, then assume first tab is desired tab
            else:
                warnings.warn(
                    f"For {file}\nyou did not include tabs, so the first tab in the file\n"
                    "will be taken as the desired data. If this is incorrect, please\n"
                    "specify the tabs you wish to use."
                )

                # extract worksheet and convert to df generator
                excel_ws = excel_wb[tab].values
                # store first row as colnames
                columns = next(excel_ws)

                # create dataframe
                df_tab = pd.DataFrame(excel_ws, columns=columns)

                # subset to desired columns
                cols_keep = [gage_id_temp] + dict_temp['tabs'][tab]['columns']

BEN WORK HERE!!!!!! - clean and finish up import of data for excel file when no tabs lists

        else:
            raise ValueError(
                    f"Incorrect extension ({extension}).\n"
                    "Allowed file extensions for input files include "
                    "[.txt, .csv, .xlsx, .xlsm]"
                )

        # add current data to dictionary of output dataframes
        dict_dfs_in[file] = df_temp

    # merge all data into a single dataframe
    df_out = dict_dfs_in[files_in[0]]
    for i, file in enumerate(files_in[1:]):
        df_out = pd.merge(
            df_out, dict_dfs_in[file],
            left_on = gage_id_cols[0],
            right_on = gage_id_cols[i+1],
            how = 'inner'
        )

    return df_out


if __name__ == '__main__':
    test1 = process_custom_attributes('./data/custom_attribute_config.yaml')
    test2 = process_custom_attributes('./data/custom_attribute_config2.yaml')
