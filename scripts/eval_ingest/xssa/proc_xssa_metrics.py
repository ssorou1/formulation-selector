'''
@title: Find CAMELS basins inside Julie Mai's xSSA dataset


'''
import pandas as pd
from pathlib import Path
import numpy as np
import yaml

# TODO create config file section for filepaths
# Define directories and paths
base_dir = '{home_dir}/noaa'.format(home_dir=str(Path.home()))
path_camels = Path(base_dir / Path('camels2/camels_hydro.txt'))
path_xssa = Path(base_dir / Path('regionalization/data/julemai-xSSA/data_in/basin_metadata/basin_validation_results.txt'))

save_dir = Path(Path(base_dir) / Path('regionalization/data/input/eval_metrics' ))

config_path = '/Users/guylitt/git/fsds/scripts/eval_ingest/xssa/xssa_schema.yaml' # TODO temp location

# ----- Read in CAMELS data (simply to retrieve the gauge_ids)
df_camlh = pd.read_csv(path_camels,sep=';',dtype={'gauge_id' :str})

# ---- Read in Julie Mai's xSSA results
df_xssa_val = pd.read_csv(path_xssa,sep = '; ',dtype={'basin_id' :str})

# Ensure appropriate str formats & remove extraneous spaces
df_xssa_val.columns = df_xssa_val.columns.str.replace(' ','')
df_xssa_val['basin_id'] = df_xssa_val['basin_id'].str.replace(' ','')

# ----- Subset the xssa dataset to CAMELS basins
df_camls_xssa_val = df_camlh.merge(df_xssa_val, left_on= 'gauge_id', right_on = 'basin_id', how='inner')
df_sub = df_camls_xssa_val.drop(columns = df_camlh.columns)

# ----- Change to standard structure: 

def read_schm_dctoflist(schema_path):
    # Load the YAML configuration file
    with open(schema_path, 'r') as file:
        config = yaml.safe_load(file)

    # Convert dict of lists into pd.DataFrame
    ls_form = list()
    for k, vv in config.items():
        for v in vv:
            ls_form.append(pd.DataFrame(v, index = [0]))
    df_all = pd.concat(ls_form, axis=1)

    return df_all


def proc_col_schema(df, col_schema_df, save_dir):

    # Based on the standardized column schema naming conventions
    metric_cols = col_schema_df['metric_cols'].iloc[0]
    gage_id = col_schema_df['gage_id'].iloc[0]
    dataset_name =  col_schema_df['dataset_name'].iloc[0]
    formulation_base =  col_schema_df['formulation_base'].iloc[0]
    formulation_id =  col_schema_df['formulation_id'].iloc[0]
    temporal_res =  col_schema_df['temporal_res'].iloc[0]
    target_var =  col_schema_df['target_var'].iloc[0]
    start_date =  col_schema_df['start_date'].iloc[0]
    end_date =  col_schema_df['end_date'].iloc[0]
    cal_yn =  col_schema_df['cal_yn'].iloc[0]
    rr =  col_schema_df['rr'].iloc[0]
    sp = col_schema_df['sp'].iloc[0]
    gw =  col_schema_df['gw'].iloc[0]
    dataset_doi =  col_schema_df['dataset_doi'].iloc[0]
    literature_doi =  col_schema_df['literature_doi'].iloc[0]
    metrics = metric_cols.split('|')



    # TODO configure which id_vars are expected in raw dataset
    df_melt_metr = pd.melt(df, id_vars = gage_id, value_vars =metrics, var_name = 'metric', value_name = 'metric_val')
    # Rename GageID
    df_melt_metr.rename(columns = {gage_id : 'gage_id'},inplace=True)

    # TODO create a function that optionally writes locally, to cloud, database, etc.
    Path(save_dir).mkdir(exist_ok=True, parents = True)
    
    # Iterate over each metric & write each one to file individually
    for metr, data_metr in df_melt_metr.groupby('metric'):
        print(f'Saving {metr} with total CAMELS basins of {data_metr.shape[0]}')
        save_path = Path(save_dir / f'{dataset_name}_{formulation_id}_{metr}.csv') # TODO is FormulationID desired in the filename
        data_metr.to_csv(save_path)

    # Write metadata table corresponding to these metric data table(s) (e.g. startDate, endDate)
    save_path_meta = Path(save_dir / f'{dataset_name}_{formulation_id}_metadata.csv')
    col_schema_df.to_csv(save_path_meta)
 
    return df_melt_metr # Returning not intended use case, but it's an option

# Read in the config file & convert to pd.DataFrame
col_schema_df = read_schm_dctoflist(schema_path = config_path)

# Extract metric data and write to file
df_melt_metr = proc_col_schema(df_sub, col_schema_df, save_dir)