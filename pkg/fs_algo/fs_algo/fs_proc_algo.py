
from sklearn.model_selection import train_test_split
import yaml
import pandas as pd
from pathlib import Path
import xarray as xr
from fs_algo.fs_algo_train_eval import AlgoTrainEval, _find_feat_srce_id, fs_retr_nhdp_comids, fs_read_attr_comid
import joblib

# Read in yaml file
path_config =  '/Users/guylitt/git/fsds/scripts/eval_ingest/xssa/xssa_attr_config.yaml' 


# attribute data of interest:
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

# base save directroy
dir_out = Path(Path(dir_base)/Path('../output/'))
dir_out.mkdir(exist_ok=True)

dir_out_alg_base = Path(dir_out/Path('trained_algorithms'))
dir_out_alg_base.mkdir(exist_ok=True)

# Identify datasets of interest:
datasets = list([x for x in config['formulation_metadata'] if 'datasets' in x][0].values())[0]

for ds in datasets: 
    print(f'PROCESSING {ds} dataset inside \n {dir_std_base}')

    # TODO implement a check to ensure each dataset directory exists
    path_nc = [x for x in Path(dir_std_base/Path(ds)).glob("*.nc") if x.is_file()]
    
    dir_out_alg_ds = Path(dir_out_alg_base/Path(ds))
    dir_out_alg_ds.mkdir(exist_ok=True)
    #path_zarr = [x for x in Path(dir_std_base/Path(ds)).glob("*.zarr") if x.is_dir()]
    try:
        dat_resp = xr.open_dataset(path_nc[0], engine='netcdf4')
    except:
        path_zarr = [x for x in Path(dir_std_base/Path(ds)).glob("*.zarr")]
        try:
            dat_resp = xr.open_dataset(path_zarr[0],engine='zarr')
        except:
            raise ValueError(f"Could not identify an approach to read in dataset via {path_nc} nor {path_zarr}")
    # The metrics approach

    metrics = dat_resp.attrs['metric_mappings'].split('|')
    gage_ids = dat_resp['gage_id'].values

    # %% COMID retrieval and assignment to response variable's coordinate
    [featureSource,featureID] = _find_feat_srce_id(dat_resp,config)
    comids_resp = fs_retr_nhdp_comids(featureSource,featureID,gage_ids)
    dat_resp = dat_resp.assign_coords(comid = comids_resp)
    # TODO allow secondary option where featureSource and featureIDs already provided, not COMID 

    #%%  Read in predictor variable data (aka basin attributes) 
    # TODO list of variables of interest:
    # TODO  Setup the s3fs filesystem that is going to be used by xarray to open the parquet files
    #_s3 = s3fs.S3FileSystem(anon=True)

    # TODO subset based on variables of interest
    # attr_arr = attr_df_sub.to_dask_array(lengths=True)
    dd_attr = fs_read_attr_comid(dir_db_attrs, comids_resp, attrs_sel = 'all',_s3 = None,storage_options=None)

    # NOTE: additional subsetting may be performed on attr_df here before computing 
    df_attr = dd_attr.compute() # Create a pandas DataFrame
    vars = df_attr['attribute'].unique()
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

        # %% TRAIN ALGORITHMS AND EVALUATE PERFORMANCE
        # Train/test split
  
        alg_cfig = algo_config.copy() # Back up the original model config before popping

        # Initialize the trainer with algo_config and metric
        trainer = AlgoTrainEval(algo_config={'rf': {'n_estimators': 100}, 
                                                'mlp': {'hidden_layer_sizes': (100,), 'max_iter': 300}},
                                metr=metr)

        def train__eval(self):

            # Train algorithms
            algs_dict = trainer.train_algos(X_train, y_train)

            # Make predictions
            preds_dict = trainer.predict_algos(X_test)

            # Evaluate predictions
            eval_dict = trainer.evaluate_algos(y_test, preds_dict)

            # Write algorithms to file
            algs_dict_paths = trainer.save_algos(ds)

            # Generate metadata dataframe
            eval_df = trainer.org_metadata_alg() # Must be called after trainer.save_algos()

        # eval_df = pd.DataFrame(eval_dict).transpose().rename_axis(index='algorithm')
        # eval_df['loc_algo'] = pd.NA
        
        


        # Record location of trained algorithm
        eval_df['loc_algo'] = [algs_dict[alg]['loc_algo'] for alg in algs_dict.keys()] 
        rslt_eval[metr] = eval_df

    rslt_eval_df = pd.concat(rslt_eval).reset_index(drop=True)
    rslt_eval_df['dataset'] = ds
    rslt_eval_df['']
    rslt_eval_df.to_parquet(Path(dir_out_alg_ds)/Path('algo_eval_'+ds+'.parquet'))
    print(f'... Finish processing {ds}')

dat_resp.close()
