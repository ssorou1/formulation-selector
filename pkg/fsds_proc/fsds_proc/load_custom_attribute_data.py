'''
Helper functions for loading attributes data. Starting with CAMELS 
    and GAGES-II attributes.
author:: Benjamin Choat <benjamin.choat@noaa.gov>
description:: given usgs catchment ID's, attributes are read in for 
    those ID's. 
notes:: developed using python v3.11.8; created 2024/08/02
'''


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


# def process_col_schema(config_schema: dict) -> pd.DataFrame:
#     for item in config_schema:
#         for key, value in item.items():
#             print(key, value)

#     return None

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

    # process_col_schema(config)
    # config = read_schm_ls_of_dict(config_file) # fails because structure/naming not as expected
    

    # process configuration file
    
    # get key, value pairs from column schema
    kv_colschema = [
         (k, v) for item in config['col_schema'] for k, v in item.items()
         ]
    # print(kv_colschema)
    
    # get list of files
    files_in = list(config['files_in'].keys())

    # loop through list and handle according to key
    # for item in config['col_schema']:
    for key, value in kv_colschema:
        print(key, value)
        if key == 'gage_ids':
            if isinstance(value, list):
                if len(value) > 1:
                    # print('is list > 1')
                    # ensure correct number of gage id columns if multiple provided
                    assert len(value) == len(files_in), \
                        (f"You provided {len(value)} values for gage_ids but "
                         f"listed {len(files_in)} files.\n" 
                         "When providing multiple id columns for gage_ids in\n"
                        "the configuration yaml file, there must be one for\n"
                        "each file. If all files have the same gage ID column\n"
                        "name, then provide a single value.")
                    
                    gage_id_cols = value

                # elif len(value) == 1:
                #     # print('is list == 1')
                #     gage_id_cols = value

            elif isinstance(value, str):
                gage_id_cols = [value]

            else:
                raise(ValueError, "gage_ids must be a single string or a list of stings.")
            print(f'gage cols: {gage_id_cols}')

        else:
            print('not gauge id')

    
    # process files listed in files_in
    for i, file in enumerate(files_in):
        print(f'i: {i}; file: {file}')
        # store config for file in dict_temp
        dict_temp = config['files_in'][file]

        # print(f'dict_temp: {dict_temp}')
        print(f'file: {file}')

        # get extension to indicate file type
        temp = file.split('.')
        extension = temp[len(temp)-1]

        # read in txt, csv, 
        if extension in ['txt', 'csv']:
            if 'separator' in dict_temp and len(dict_temp['separator']) == 1:
                separator = dict_temp['separator'][0]

                # read in file
                try:
                    df_temp = pd.read_csv(file, sep=separator)
                except:
                    raise
            else:
                warnings.warn(
                    f"separator was not provided for \n{file} in config "
                    " yaml file. \nso attempting to identify automatically."
                )

                # None uses python csv sniffer to id separator
                try:
                    df_temp = pd.read_csv(file, sep=None, engine='python')
                except:
                    raise

            # check that listed columns match df_temp columns
            # id columns listed in config yaml if they are not in df_temp
            missing_config_cols = [
                x for x in dict_temp['columns'] if x not in df_temp.columns
            ]
            
            # if there are missing columns, return error
            if len(missing_config_cols) > 0:
                raise ValueError(
                    "In the configuration yaml file the following\n"
                    f"columns were listed, but they do not appear in\n"
                    f"{file}:\n{missing_config_cols}"
                )
            
            # check specified gage_id column is present
            if (len(gage_id_cols) == 1 and 
                    gage_id_cols[0] not in df_temp.columns):
                raise ValueError(
                    f"{gage_id_cols[0]} was specified to be the name of the\n"
                    "id column in the configuration file, but there does not\n"
                    f"seem to be a column with that name in {file}."
                )

            elif (len(gage_id_cols) > 1 and 
                    gage_id_cols[i] not in df_temp.columns):
                warnings.warn(
                    f"{gage_id_cols[i]} was not found in {file}, so trying\n"
                    "the other values listed as gage_ids: "
                    f"{[x for x in gage_id_cols if x != gage_id_cols[i]]}."
                )

                # id other listed gage_ids that appear in current file
                ids_present = [x for x in gage_id_cols if x in df_temp.columns]

                if len(ids_present) == 0:
                    raise ValueError(
                    f"None of the specified gage_id column names appear to be\n"
                    f"a column in {file}.\n"
                    'Please double check your list of gage_ids: '
                    f'{gage_id_cols}'
                )

            
            # else:
                handle case where gage_id_cols is correct

            

        elif extension == 'xlsx':
            print('hold')

        del(temp)
        print(extension)


    return  config # kv_infiles#


if __name__ == '__main__':
    test = process_custom_attributes('./data/custom_attribute_config.yaml')








# ################################
# # maybe delete below here

# def download_camels(dest_path: str | os.PathLike,
#                     Download_Timeseries: bool = False) -> Path:
#     print("They are not here.")
#     # use config file; What does not exist, then hops over to attribute grabber.
#     return(None)

# def process_camels(input_path: str | os.PathLike,
#                    output_path: str | os.PathLike,  
#                    ids: list, 
#                    attibutes: list) -> pd.DataFrame:
#     '''
#     Given a directory holding CAMELS files and gauge-id's, this function
#     loads CAMELS data and subsets it to the gauge-ids.
#     NOTE: Function expects `path` to point to a folder holding all 
#     CAMELS data, and expects that data to be formatted as it is when 
#     the .zip file is downloaded from:
#     https://gdex.ucar.edu/dataset/camels.html#:~:text=Individual%20Files%20%2D-,View,-%2C%20select%2C%20and%20download
#     and extracted to `path`.

#     Inputs
#     -------------
#     path (str or path): path to folder holding CAMELS data.
#     ids (list): list of gauge-ids associated with CAMELS data and 
#         to be extracted when this function is executed.
#         Be careful that leading 0's are present in ids.


#     Outputs
#     --------------
#     df (pandas DataFrame): 
    
#     '''

#     return None


# def process_gagesii(input_path: str | os.PathLike, 
#                  output_path: str | os.PathLike, 
#                  ids: list, 
#                  attibutes: list) -> pd.DataFrame:
#     '''
#     Given a directory holding GAGES-II files and gauge-id's, this function
#     loads CAMELS data and subsets it to the gauge-ids.
#     NOTE: Function expects `path` to point to a folder holding all 
#     CAMELS data, and expects that data to be formatted as it is when 
#     the .zip file is downloaded from:
#     https://gdex.ucar.edu/dataset/camels.html#:~:text=Individual%20Files%20%2D-,View,-%2C%20select%2C%20and%20download
#     and extracted to `path`.

#     Inputs
#     -------------
#     path (str or path): path to folder holding CAMELS data.
#     ids (list): list of gauge-ids associated with CAMELS data and 
#         to be extracted when this function is executed.
#         Be careful that leading 0's are present in ids.


#     Outputs
#     --------------
#     df (pandas DataFrame): 
    
#     '''

#     return None
