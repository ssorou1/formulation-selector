import argparse
import yaml
import joblib
import fs_algo.fs_algo_train_eval as fsate
import pandas as pd
from pathlib import Path
import ast
# Predict values and evaluate predictions


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'process the algorithm config file')
    parser.add_argument('path_algo_config', type=str, help='Path to the YAML configuration file specific for algorithm training')
    # TODO parser.add_argument path_pred_config
    # NOTE pred_config should contain the path for path_algo_config
    args = parser.parse_args()

    path_algo_config = Path(args.path_algo_config) #Path('/Users/guylitt/git/formulation-selector/scripts/eval_ingest/xssa/xssa_algo_config.yaml') 
    
    with open(path_algo_config, 'r') as file:
        algo_cfg = yaml.safe_load(file)

    algo_config = {k: algo_cfg['algorithms'][k][0] for k in algo_cfg['algorithms']}
    if algo_config['mlp']['hidden_layer_sizes']: # purpose: evaluate string literal to a tuple
        algo_config['mlp']['hidden_layer_sizes'] = ast.literal_eval(algo_config['mlp']['hidden_layer_sizes'])
    
    home_dir = Path.home()

    #%%  Read the attribute configuration file corresponding to the algorithm training
    try:
        name_attr_config = algo_cfg.get('name_attr_config', Path(path_algo_config).name.replace('algo','attr'))
    except:
        print("TODO: allow attr_config reference to be specified in the prediction config, OR")
        print("OR specify the needed objects in the prediction config")
    path_attr_config = Path(Path(path_algo_config).parent/name_attr_config)
    if not Path(path_attr_config).exists():
        raise ValueError(f"Ensure that 'name_attr_config' as defined inside {path_algo_config.name} \
                          \n is also in the same directory as the algo config file {path_algo_config.parent}" )
    
    print("BEGINNING prediction of algorithm.")

    attr_cfig = fsate.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()

    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    dir_out = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out')
    
    dir_out_alg_base = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out_alg_base')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets') # Identify datasets of interest

    #%% prediction config
    # TODO create pred config
    # path_pred_config = Path(args.path_pred_config)
    metrics = ['KGE'] 
    algos = ['rf','mlp']

    dir_base = f'{home_dir}/noaa/regionalization/data/input' # Required. The save location of standardized output
    dir_std_base = 
    path_pred_locs = f'{dir_std_base}/prediction_locations_comid.csv'

    

    for ds in datasets:
        dir_out_alg_ds = Path(dir_out_alg_base/Path(ds))

        # TODO read algo evaluation metadata
        # df_meta = pd.read_parquet(dir_out_alg_base.str.replace('.joblib','.parquet'))
        path_eval_dat =  str(fsate.std_algo_path(dir_out_alg_ds, algo='', metric='eval',
                                                 dataset_id=ds)).replace("__eval__",'_eval_').replace('.joblib','.parquet')
        pd.read_parquet(path_eval_dat)
        for metric in metrics:
            for algo in algos:
                path_algo = fsate.std_algo_path(dir_out_alg_ds, algo=algo, metric=metric, dataset_id=ds)

                # TODO consider reading in .parquet metadata first
