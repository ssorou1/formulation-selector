'''
Functions for loading attributes data. Starting with CAMELS 
    and GAGES-II attributes.
author:: Benjamin Choat <benjamin.choat@noaa.gov>
description:: given usgs catchment ID's, attributes are read in for 
    those ID's. 
notes:: developed using python v3.11.8; created 2024/08/02
'''

# TODO: 
# - if list of gage_ids provided, then first subset dfs before merging
# - modularize 1. txt, csv read, 2. xcl read, 3. check config as expected (Not sure 3 is needed)
# - keep only one id column (currently multiple are kept if different colnames are provided)

# import libraries needed across all functions
import pandas as pd
from glob import glob
import pandas as pd
from pathlib import Path
import yaml
import warnings
import os
import shutil
from fsds_proc import data
from openpyxl import load_workbook


######################################################################

def check_config_valid(config_main):

    # check 1st layer of keys seem correct

    # define list of expected keys
    keys_expected = [
        'output_file', 'files_in', 'attribute_metadata', 'references'
        ]
    
    # read 1st layer keys
    keys_temp = list(config_main.keys())

    # if keys_expected and keys_temp don't match, check which keys are different
    if keys_expected != keys_temp:
        in_expected = [x for x in keys_expected if x not in keys_temp]
        in_temp = [x for x in keys_temp if x not in keys_expected]

        if len(in_expected) > 0:
            message_out1 = (
                "\nThe following key is missing at the first level in the "
                "configuration file:\n"
                f"{in_expected}."
            )
        else:
            message_out1 = ""
        
        if len(in_temp) > 0:
            message_out2 = (
                "\nThe following keys were provided at the first level in "
                "the configuration file, but should not be:\n"
                f"    {in_temp}\n"
            )
        else:
            message_out2 = ""

        if len(in_expected) > 0 or len(in_temp) > 0:
            raise ValueError(f'{message_out1}\n{message_out2}')
    
    # check output file provided
    if len(config_main['output_file']) != 1:
        message_out = (
            "There should be a single path provided defining where to save "
            f"the output.\n    You provided: {config_main['output_file']}"
        )
        raise ValueError(message_out)
    
    # check input files seem correct

    # get listed files

    # define list of required keys for .txt/.csv and .xlsx/.xlsm
    txtcsv_accepted = ['separator', 'id_column', 'attr_columns']
    xl_accepted = ['tabs', 'id_column', 'attr_colums']

    listed_files = list(config_main['files_in'].keys())

    # messages will be updated as errors are found
    message_out1 = ''
    message_out2 = ''

    for file in listed_files:
        extension = file.split('.')[len(file.split('.'))-1]
        keys_temp = list(config_main['files_in'][file].keys())
        if extension in ['txt', 'csv']:
            in_expected = [x for x in txtcsv_accepted if x not in keys_temp]
            in_temp = [x for x in keys_temp if x not in txtcsv_accepted]

            if len(in_expected) == 1 and in_expected[0] == 'separator':
                warnings.warn(
                    f"\nseparator was not provided for {file}\n"
                    f"so the code will try to detect it automatically."
                )
            if ((len(in_expected) == 1 and in_expected[0] != 'separator') or
                    len(in_expected) > 1):
                message_temp = (
                    'You did not provide the following expected keys for \n'
                    f'  {file}:\n    {in_expected}'
                )
                message_out1 = f'{message_out1}{message_temp}'
            if len(in_temp) > 0:
                message_temp = (
                    "You provided the following keys in the configuration file,\n"
                    f"for {file}\n"
                    f"but they are not accepted:\n{in_temp}\n"
                )
                message_out2 = message_temp

        elif extension in ['xlsx', 'xlsm']:
            in_expected = [x for x in xl_accepted if x not in keys_temp]
            in_xl = [x for x in keys_temp if x not in xl_accepted]

            if len(in_expected) == 1 and in_expected[0] == 'tabs':
                warnings.warn(
                    f"\ntabs were not provided for {file} so data in\n"
                    "the first tab will be used."
                )
            if ((len(in_expected) == 1 and in_expected[0] != 'tabs') or
                    len(in_expected) > 1):
                message_temp = (
                    'You did not provide the following expected keys for \n'
                    f'  {file}:\n    {in_expected}\n'
                )
                message_out2 = message_out2
            else:
                message_out2 = ''
        else:
            raise ValueError(
                    f"Incorrect extension ({extension}).\n"
                    "Allowed file extensions for input files include "
                    "[.txt, .csv, .xlsx, .xlsm]"
                )

        if len(message_out1) > 0 or len(message_out2) > 0:
            raise ValueError(f'{message_out1}\n{message_out2}')
            
        # handle subkeys of tabs if present
        if 'tabs' in keys_temp:
            temp_dict = config_main['files_in'][file]['tabs']
            tabs_expected = ['id_column', 'attr_columns']
            for tab in temp_dict.keys():
                temp_keys = list(temp_dict[tab].keys())
                in_expected = [
                    x for x in tabs_expected if x not in temp_keys
                    ]
                in_tab = [x for x in temp_keys if x not in tabs_expected]

                if len(in_expected) > 0:
                    message_out1 = (
                        f"The following keys are missing for {file}:\n"
                        f"   {in_expected}"
                        )
                else:
                    message_out1 = ''
                
                if len(in_tab) > 0:
                    message_out2 = (
                        f"The following keys were provided for {file}\n but "
                        f"    were not expected:\n{in_tab}"
                    )
                else:
                    message_out2 = ''
                
                if len(in_expected) > 0 or len(in_tab) > 0:
                    raise ValueError(f'{message_out1}\n{message_out2}')
                



    

    # # get key, value pairs from column schema
    # kv_colschema = [
    #      (k, v) for item in config_main['col_schema'] for k, v in item.items()
    #      ]
    # # loop through list and handle according to key
    # for key, value in kv_colschema:
    #     print(f'key: {key}\nvalue: {value}')
    #     if key == 'gage_ids':
    #         None


    # # tabs present for non-xcel file? should not be
    # # separator present for excel file? should not be
    # # each file/tab has id specified?
    # # keys are valid at each level

    # files_in = list(dict_config['files_in'].keys())

    # # check if tabs listed, if so treat tabs as files
    # tab_files = [x for x in files_in if 'tabs' in dict_config['files_in'][x]]

    # # check if any non-excel files contain 'tabs', they should not
    # tab_nonxlsx = [x for x in tab_files if \
    #                 x.split('.')[len(x.split('.'))-1] not in ['xlsx', 'xlsm']]
    # if len(tab_nonxlsx) > 0:
    #     raise ValueError(
    #                 "In configuration file, 'tabs' should only be included for\n"
    #                 "   .xlsx or .xlsm files but you've included 'tabs' for \n" 
    #                 f"  {tab_nonxlsx}."
    #             )

    return None

def check_columns_present(dict_config: dict, 
                          df_data: pd.DataFrame, 
                          gage_id_col: str, 
                          file: str | os.PathLike) -> str:
    '''
    Function checks that columns specified in dict_config are present
    in df_data. First checks if id column is present, then checks
    if attribute columns are present.
    If specified columns are not present then an error is thrown.
    
    Inputs
    -------------------
    dict_config (dict): dictionary generated when custom_attribute_config.yaml
        is read in. 
    df_data (pandas DataFrame): a pandas dataframe expected to contain columns
        with a gage_id column name and attribute column names specified in 
        dict_config
    gage_id_col (str): Name of column that holds values to be used as IDs
    file (str): name of file from which df_data was read in

    Outputs
    -------------------
    message_id (str): Not intneded to be used as output, but instead printed
        during processing
    message_attr (str): Not intneded to be used as output, but instead printed
        during processing

    '''
    # check specified gage_id column is present in current file
    if gage_id_col not in df_data.columns:
        raise ValueError(
            f"In the configuration file '{gage_id_col}'\n"
            "   was specified to be the name of the\n"
            "   id column, but there does not"
            f"  seem to be a column\nwith that name in {file}."
        )

    else:
        message_id = f'\n"{gage_id_col}" used as gage_id column in {file}.\n'


    # id columns listed in config yaml if they are not in current file
    missing_config_cols = [
        x for x in dict_config['attr_columns'] if x not in df_data.columns
    ]
    
    # if there are missing columns, return error
    if len(missing_config_cols) > 0:
        raise ValueError(
            "In the configuration yaml file the following\n"
            f"  columns were listed, but they do not appear in\n"
            f"  {file}:\n{missing_config_cols}"
        )
    else:
        message_attr = f"\nAll specified columns for {file} are present\n"

    print(message_id)
    print(message_attr)
    return message_id, message_attr


######################################################################


def process_txt_csv(dict_config: dict, file: str | os.PathLike) -> pd.DataFrame:
    '''
    Read in file and process according to inputs in dict_config

    Inputs
    ----------------
    dict_config (dict): dictionary generated when custom_attribute_config.yaml
        is read in. 
    file (str): name of file from which data will be read in

    Outputs
    ----------------
    df (pd.DataFrame): Dataframe containing data from file and processed 
        according to dict_config
    '''


    print("processing txt or csv")

    # get gage_id column name so it can be read in as string in case
    # there are leading 0's
    gage_id_temp = dict_config['id_column'][0]

    # extract separator if provided
    if 'separator' in dict_config and len(dict_config['separator']) == 1:
        separator = dict_config['separator'][0]

        # read in file
        try:
            df = pd.read_csv(
                file, 
                sep=separator,
                dtype={gage_id_temp: str},
                usecols=[gage_id_temp] + dict_config['attr_columns']
                )
        except:
            raise
    else:
        warnings.warn(
            f"\nseparator was not provided for \n{file} in config "
            " yaml file. \n    so attempting to identify automatically."
        )

        # sep=None uses python csv sniffer to id separator
        try:
            df = pd.read_csv(
                file, 
                sep=None, 
                engine='python',
                dtype={gage_id_temp: str},
                usecols=[gage_id_temp] + dict_config['attr_columns']
                )
        except:
            raise

    # check that listed columns match current file columns
    check_columns_present(dict_config,
                        df,
                        gage_id_temp,
                        file)
    
    return df


######################################################################


def process_excel(dict_config: dict, 
                  file: str | os.PathLike,
                  tabs_included: bool) -> tuple[pd.DataFrame, list]:
    '''
    Read in file and process according to inputs in dict_config

    Inputs
    ----------------
    dict_config (dict): dictionary generated when custom_attribute_config.yaml
        is read in. 
    file (str): name of file from which data will be read in
    tabs_included (boolean): True if tabs provided in config yaml, otherwise False

    Outputs
    ----------------
    df (pd.DataFrame): Dataframe containing data from file and processed 
        according to dict_config
    gage_id_cols (list): A list of gage_ids associated with each file or tab
    '''


    print("processing xlsx or xlsm")

    
    # load excel workbook
    try:
        excel_wb = load_workbook(
                        filename=file,
                        data_only = True,
                        read_only=True
                        )
    except:
        raise

    if tabs_included:

        # get list of tabs associated with current file
        tabs_temp = list(dict_config['tabs'].keys())

        # create dict to hold gage_id_column for each tab-used for mergin in end
        dict_gg_temp = {x: [] for x in tabs_temp}

        # create empty list to store data from tabs in
        dict_dfs_tabs = {tab: [] for tab in tabs_temp}
        for j, tab in enumerate(tabs_temp):
            
            # # get gage_id column name so it can be read in as string in case
            # # there are leading 0's
            gage_id_temp = dict_config['tabs'][tab]['id_column'][0]

            # create or append gage_id col to list to be used later when
            # merging data
            if j == 0:
                gage_id_cols = [gage_id_temp]
            else:
                gage_id_cols.append(gage_id_temp)

            # extract worksheet and convert to df generator
            excel_ws = excel_wb[tab].values
            # store first row as colnames
            colnames = next(excel_ws)

            # create dataframe
            df_tab = pd.DataFrame(excel_ws, columns=colnames)

            check_columns_present(dict_config['tabs'][tab],
                                df_tab,
                                gage_id_temp,
                                file)

            # subset to desired columns
            cols_keep = (
                [gage_id_temp] + dict_config['tabs'][tab]['attr_columns']
                )

            # add current dataframe to tabs output dict
            dict_dfs_tabs[tab] = df_tab[cols_keep]

            # add gage_id to dict holding gage_id for each tab
            dict_gg_temp[tab] = gage_id_temp

            # merge dataframes using gage_ids
            df = dict_dfs_tabs[tabs_temp[0]]

        for tab in tabs_temp[1:]:
            df = pd.merge(
                df, dict_dfs_tabs[tab],
                left_on = dict_gg_temp[tabs_temp[0]],
                right_on = dict_gg_temp[tab],
                how = 'inner'
            )

    # if xlsx or xlsm and tab not included, then assume first tab is desired tab
    else:
        warnings.warn(
            f"\nFor {file}\n    you did not include tabs, so the first "
            "tab in the file\n   will be taken as the desired data. If "
            "this is incorrect, please\n"
            "   specify the tabs you wish to use."
        )

        # get gage_id column name so it can be read in as string in case
        # there are leading 0's
        print(f'dict_config: {dict_config}')
        gage_id_temp = dict_config['id_column'][0]

        gage_id_cols = gage_id_temp

        # extract worksheet and convert to df generator
        excel_ws = excel_wb.worksheets[0].values
        # store first row as colnames
        colnames = next(excel_ws)

        # create dataframe
        df = pd.DataFrame(excel_ws, columns=colnames)

        check_columns_present(dict_config,
                            df,
                            gage_id_temp,
                            file)

        # subset to desired columns
        cols_keep = ([gage_id_temp] 
            + dict_config['attr_columns'])
        
        # subet df to cols_keep
        df = df[cols_keep]
    
    return df, gage_id_cols


######################################################################


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

    # check configuration file seems correct
    check_config_valid(config)

    # process configuration file    
    
    # get list of files
    files_in = list(config['files_in'].keys())

    #####
    # make sure config file is as expected
    ####

    # define empty dict to append data to as read in
    dict_dfs_in = {f: [] for f in files_in}

    # process files listed in files_in
    for i, file in enumerate(files_in):
        
        print(f'Processing file {i+1} of {len(files_in)}\nfile: {file}')

        # store config for file in dict_temp
        dict_temp = config['files_in'][file]

        # get extension to indicate file type
        temp = file.split('.')
        extension = temp[len(temp)-1]

        # read in txt, csv, 
        if extension in ['txt', 'csv']:

            # create or append gage_id col to list to be used later when
            # merging data
            if i == 0:
                gage_id_cols = [dict_temp['id_column'][0]]
            else:
                gage_id_cols.append(dict_temp['id_column'][0])

            df_temp = process_txt_csv(dict_temp, file)

        else: # if extension in ['xlsx', 'xlsm']:

            # check if current file included tabs; if not then assume first tab is desired
            tabs_included = True if 'tabs' in config['files_in'][file] else False

            # process the file
            df_temp, gage_id_temp = process_excel(
                                        dict_temp,
                                        file,
                                        tabs_included
                                    )
            
            # create or append gage_id col to list to be used later when
            # merging data
            if i == 0:
                gage_id_cols = gage_id_temp
            else:
                gage_id_cols.extend(gage_id_temp)


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

    print(config['output_file'][0])
    df_out.to_csv(config['output_file'][0], index=False)
    print(f"\nOutput file saved to {config['output_file'][0]}\n")

    # return df_out
    return config


######################################################################


if __name__ == '__main__':
    test1 = process_custom_attributes('data/custom_attribute_config.yaml')
    # test2 = process_custom_attributes('data/custom_attribute_config2.yaml')
    # test3 = process_custom_attributes('data/custom_attribute_config3.yaml')
