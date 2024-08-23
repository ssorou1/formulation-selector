import yaml
import pandas as pd
from pathlib import Path
import xarray as xr
import fs_algo.fs_algo_train_eval as fsate

print("BEGINNING algorithm training, testing, & evaluation.")

# Read in yaml file
path_config =  '/Users/guylitt/git/fsds/scripts/eval_ingest/xssa/xssa_attr_config.yaml' 




# attribute data of interest:
vars = None # TODO allow user to define basin attributes of interest, otherwise default None

# Attribute data location:
with open(path_config, 'r') as file:
    config = yaml.safe_load(file)

# TODO create a model configuration file
algo_config = {'rf': {'n_estimators':100},
                    'mlp': {'hidden_layer_sizes' :(4,),
                             'activation':'relu',
                             'solver':'lbfgs',
                             'alpha':0.001,
                             'batch_size':'auto',
                             'learning_rate':'constant',
                             'power_t':0.5,
                             'max_iter':20000,
                             }}


home_dir = str(Path.home())
dir_base = list([x for x in config['file_io'] if 'dir_base' in x][0].values())[0].format(home_dir=home_dir)
# Location of attributes (predictor data):
dir_db_attrs = list([x for x in config['file_io'] if 'dir_db_attrs' in x][0].values())[0].format(dir_base = dir_base)

# parent location of response variable data:
dir_std_base =  list([x for x in config['file_io'] if 'dir_std_base' in x][0].values())[0].format(dir_base = dir_base)



# Generate standardized output directories
out_dirs = fsate.fs_save_algo_dir_struct(dir_base)
dir_out = out_dirs.get('dir_out')
dir_out_alg_base = out_dirs.get('dir_out_alg_base')


config_paths = {'dir_std_base': dir_std_base,}

#def prep_fsds_attr_ds(ds)
def _open_response_data_fsds(dir_std_base,ds):
    # TODO implement a check to ensure each dataset directory exists
    path_nc = [x for x in Path(dir_std_base/Path(ds)).glob("*.nc") if x.is_file()]

    try:
        dat_resp = xr.open_dataset(path_nc[0], engine='netcdf4')
    except:
        path_zarr = [x for x in Path(dir_std_base/Path(ds)).glob("*.zarr")]
        try:
            dat_resp = xr.open_dataset(path_zarr[0],engine='zarr')
        except:
            raise ValueError(f"Could not identify an approach to read in dataset via {path_nc} nor {path_zarr}")
    return dat_resp



# Identify datasets of interest:
datasets = list([x for x in config['formulation_metadata'] if 'datasets' in x][0].values())[0]

for ds in datasets: 
    print(f'PROCESSING {ds} dataset inside \n {dir_std_base}')

    
    # TODO implement a check to ensure each dataset directory exists
    dir_out_alg_ds = Path(dir_out_alg_base/Path(ds))
    dir_out_alg_ds.mkdir(exist_ok=True)

    # # TODO implement a check to ensure each dataset directory exists
    # path_nc = [x for x in Path(dir_std_base/Path(ds)).glob("*.nc") if x.is_file()]

    # #path_zarr = [x for x in Path(dir_std_base/Path(ds)).glob("*.zarr") if x.is_dir()]
    # try:
    #     dat_resp = xr.open_dataset(path_nc[0], engine='netcdf4')
    # except:
    #     path_zarr = [x for x in Path(dir_std_base/Path(ds)).glob("*.zarr")]
    #     try:
    #         dat_resp = xr.open_dataset(path_zarr[0],engine='zarr')
    #     except:
    #         raise ValueError(f"Could not identify an approach to read in dataset via {path_nc} nor {path_zarr}")
    
    dat_resp = _open_response_data_fsds(dir_std_base,ds)

    # The metrics approach
    metrics = dat_resp.attrs['metric_mappings'].split('|')
    gage_ids = dat_resp['gage_id'].values


    # %% COMID retrieval and assignment to response variable's coordinate
    [featureSource,featureID] = fsate._find_feat_srce_id(dat_resp,config)
    comids_resp = fsate.fs_retr_nhdp_comids(featureSource,featureID,gage_ids)
    dat_resp = dat_resp.assign_coords(comid = comids_resp)
    # TODO allow secondary option where featureSource and featureIDs already provided, not COMID 

    #%%  Read in predictor variable data (aka basin attributes) 
    # TODO list of variables of interest:
    # TODO  Setup the s3fs filesystem that is going to be used by xarray to open the parquet files
    #_s3 = s3fs.S3FileSystem(anon=True)

    # TODO subset based on variables of interest
    # attr_arr = attr_df_sub.to_dask_array(lengths=True)
    dd_attr = fsate.fs_read_attr_comid(dir_db_attrs, comids_resp, attrs_sel = 'all',
                                       _s3 = None,storage_options=None)

    # NOTE: additional subsetting may be performed on attr_df here before computing 
    df_attr = dd_attr.compute() # Create a pandas DataFrame
    if not vars: # In case the user doesn't specify variables, grab them all
        vars = df_attr['attribute'].unique() # Extract the catchment attributes of interest
        # TODO run check that all vars are present for all basins

    df_attr_wide = df_attr.pivot(index='featureID', columns = 'attribute', values = 'value')
    dd_resp = dat_resp.to_dask_dataframe()
    
    #%% Join attribute data and response data
    rslt_eval = dict()
    for metr in metrics:
        print(f' - Processing {metr}')
        # Subset response data to metric of interest & the comid
        df_metr_resp = pd.DataFrame({'comid': dat_resp['comid'],
                                     metr : dat_resp[metr].data})
        
        df_pred_resp = df_metr_resp.merge(df_attr_wide, left_on = 'comid', right_on = 'featureID')


        train_eval = fsate.AlgoTrainEval(df=df_pred_resp,
                                     vars=vars,algo_config=algo_config,
                                     dir_out_alg_ds=dir_out_alg_ds, dataset_id=ds,
                                     metr=metr,test_size=0.7, rs = 32)
        
        # Run the training, testing, and evaluation wrapper:
        train_eval.train_eval()
        
        # Retrieve evaluation metrics dataframe
        rslt_eval[metr] = train_eval.eval_df

    rslt_eval_df = pd.concat(rslt_eval).reset_index(drop=True)
    rslt_eval_df['dataset'] = ds
    rslt_eval_df.to_parquet(Path(dir_out_alg_ds)/Path('algo_eval_'+ds+'.parquet'))
    print(f'... Finish processing {ds}')

dat_resp.close()
print("FINISHED algorithm training, testing, & evaluation")