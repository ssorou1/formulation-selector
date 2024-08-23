from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd
import xarray as xr
import pynhd as nhd
import dask_expr
import dask.dataframe as dd
import os
from collections.abc import Iterable
from typing import List, Optional, Dict
from pathlib import Path
import joblib
import itertools
import yaml
import warnings

# %% BASIN ATTRIBUTES (PREDICTORS) & RESPONSE VARIABLES (e.g. METRICS)
class AttrConfigAndVars:
    def __init__(self, path_attr_config: str | os.PathLike):
        self.path_attr_config = path_attr_config
        self.attrs_cfg_dict = dict()
        self.attr_config = dict()

    def _read_attr_config(self ) -> dict:
        # Extract the desired basin attribute variable names from yaml file

        # Attribute data location:
        with open(self.path_attr_config, 'r') as file:
            self.attr_config = yaml.safe_load(file)

        # identify attribute data of interest from attr_config
        attrs_all = [v for x in self.attr_config['attr_select'] for k,v in x.items() if '_vars' in k]
        attrs_sel = [x for x in list(itertools.chain(*attrs_all)) if x is not None]

        if len(attrs_sel) == None: # If no attributes generated, assume all attributes are of interest
            attrs_sel = 'all'
            raise warnings.warn(f"No attributes discerned from 'attr_select' inside ")
        
        home_dir = str(Path.home())
        dir_base = list([x for x in self.attr_config['file_io'] if 'dir_base' in x][0].values())[0].format(home_dir=home_dir)
        # Location of attributes (predictor data):
        dir_db_attrs = list([x for x in self.attr_config['file_io'] if 'dir_db_attrs' in x][0].values())[0].format(dir_base = dir_base)

        # parent location of response variable data:
        dir_std_base =  list([x for x in self.attr_config['file_io'] if 'dir_std_base' in x][0].values())[0].format(dir_base = dir_base)

        # The datasets of interest
        datasets = list([x for x in self.attr_config['formulation_metadata'] if 'datasets' in x][0].values())[0]
        # Compile output
        self.attrs_cfg_dict = {'attrs_sel' : attrs_sel,
                            'dir_db_attrs': dir_db_attrs,
                            'dir_std_base': dir_std_base,
                            'dir_base': dir_base,
                            'datasets': datasets}


def fs_read_attr_comid(dir_db_attrs:str | os.PathLike, comids_resp:list, attrs_sel = 'all',
                       _s3 = None,storage_options=None)-> dask_expr._collection.DataFrame:
    if _s3:
        storage_options={"anon",True} # for public
        # TODO  Setup the s3fs filesystem that will be used, with xarray to open the parquet files
        #_s3 = s3fs.S3FileSystem(anon=True)

    # Read attribute data acquired using fsds.attr.hydfab R package
    all_attr_df = dd.read_parquet(dir_db_attrs, storage_options = storage_options)

    # Subset based on comids of interest
    attr_df_subloc = all_attr_df[all_attr_df['featureID'].str.contains('|'.join(comids_resp))]

    if attrs_sel == 'all':
        # TODO shold figure out which attributes are common across all data when using 'all'
        attrs_sel = attr_df_subloc['attribute'].unique().compute()

    attr_df_sub = attr_df_subloc[attr_df_subloc['attribute'].str.contains('|'.join(attrs_sel))]
    return attr_df_sub

def _check_attributes_exist(df_attr: pd.DataFrame, vars:pd.Series | Iterable):
    # Run check that all vars are present for all basins
    if df_attr.groupby('featureID')['attribute'].count().nunique() != 1:
        vec_missing = df_attr.groupby('featureID')['attribute'].count() != len(vars)
        bad_comids = vec_missing.index.values
        
        df_attr_sub_missing = df_attr[df_attr['featureID'].isin(bad_comids)]

        missing_vars = vars[~vars.isin(df_attr_sub_missing['attribute'])]

        warn_msg_missing_vars = f"Not all featureID groupings (i.e. COMID groups) contain \
        the same number of catchment attributes. \
        \n This could be problematic for model training. \
        \n Consider running attribute grabber with fsds.attr.hydfab. \
        \n Missing attributes include: {', '.join(missing_vars)}"

        raise warnings.warn(warn_msg_missing_vars)




def _find_feat_srce_id(dat_resp: Optional[xr.core.dataset.Dataset] = None,
                       attr_config: Optional[Dict] = None) -> List[str]:
    # Attempt to grab dataset attributes (in cases where it differs by dataset), fallback on config file
    featureSource = None
    try: # dataset attributes first
        featureSource = dat_resp.attrs.get('featureSource', None)
    except (KeyError, StopIteration): # config file second
        featureSource = next(x['featureSource'] for x in attr_config['col_schema'] if 'featureSource' in x)
    if not featureSource:
        raise ValueError(f'The featureSource could not be found. Ensure it is present in the col_schema section {path_config}')
    # Attempt to grab featureID from dataset attributes, fallback to the config file
    featureID = None
    try: # dataset attributes first
        featureID = dat_resp.attrs.get('featureID', None)
    except (KeyError, StopIteration): # config file second
        featureID = next(x['featureID'] for x in attr_config['col_schema'] if 'featureID' in x)
    if not featureID:
        raise ValueError(f'The featureID could not be found. Ensure it is present in the col_schema section of {path_config}')
        # TODO need to map gage_id to location identifier in attribute data!

    return [featureSource, featureID]

def fs_retr_nhdp_comids(featureSource:str,featureID:str,gage_ids: Iterable[str] ) ->list:    

    # Retrieve response variable's comids, querying the shortest distance in the flowline
    nldi = nhd.NLDI()
    comids_resp = [nldi.navigate_byid(fsource=featureSource,fid= featureID.format(gage_id=gage_id),
                                navigation='upstreamMain',
                                source='flowlines',
                                distance=1 # the shortest distance
                                ).loc[0]['nhdplus_comid'] 
                                for gage_id in gage_ids]
    
    if len(comids_resp) != len(gage_ids) or comids_resp.count(None) > 0:
        raise warnings.warn("The total number of retrieved comids does not match \
                      total number of provided gage_ids")

    return comids_resp


def fs_save_algo_dir_struct(dir_base: str | os.PathLike ) -> dict:

    if not Path(dir_base).exists():
        raise ValueError(f"The provided dir_base does not exist. \
                         \n Double check the config file to make sure \
                         an existing directory is provided. dir_base= \
                         \n{dir_base}")

    # Define the standardized directory structure for algorithm output
    # base save directory
    dir_out = Path(Path(dir_base).parent.absolute()/Path('output'))
    dir_out.mkdir(exist_ok=True)

    # The trained algorithm directory
    dir_out_alg_base = Path(dir_out/Path('trained_algorithms'))
    dir_out_alg_base.mkdir(exist_ok=True)

    out_dirs = {'dir_out': dir_out,
                'dir_out_alg_base': dir_out_alg_base}

    return out_dirs

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

# %% ALGORITHM TRAINING AND EVALUATION
class AlgoTrainEval:
    def __init__(self, df: pd.DataFrame, vars: Iterable[str], algo_config: dict,
                 dir_out_alg_ds: str | os.PathLike, dataset_id: str,
                 metr: str = None, test_size: float = 0.7,rs: int = 32,
                 verbose: bool = False):
        # class args
        self.df = df
        self.vars = vars
        self.algo_config = algo_config
        self.dir_out_alg_ds = dir_out_alg_ds
        self.metric = metr
        self.test_size = test_size
        self.rs = rs
        self.dataset_id = dataset_id
        self.verbose = verbose

        # train/test split
        self.X_train = pd.DataFrame()
        self.X_test = pd.DataFrame()
        self.y_train = pd.Series()
        self.y_test = pd.Series()
        
        # train/pred/eval metadata
        self.algs_dict = {}
        self.preds_dict = {}
        self.eval_dict = {}

        # The evaluation summary result
        self.eval_df = pd.DataFrame()
    def split_data(self):
        if self.verbose:
            print(f"      Performing train/test split as {self.test_size}/{round(1-self.test_size,2)}")

        X = self.df[self.vars]
        y = self.df[self.metric]
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(X,y, test_size=self.test_size, random_state=self.rs)
        

    def train_algos(self):

        # Train algorithms based on config
        if 'rf' in self.algo_config:  # RANDOM FOREST
            if self.verbose:
                print(f"      Performing Random Forest Training")
            rf = RandomForestRegressor(n_estimators=self.algo_config['rf'].get('n_estimators'),
                                       random_state=self.rs)
            rf.fit(self.X_train, self.y_train)
            self.algs_dict['rf'] = {'algo': rf,
                                    'type': 'random forest regressor',
                                    'metric': self.metric}

        if 'mlp' in self.algo_config:  # MULTI-LAYER PERCEPTRON
            if self.verbose:
                print(f"      Performing Multilayer Perceptron Training")
            mlpcfg = self.algo_config['mlp']
            mlp = MLPRegressor(random_state=self.rs,
                               hidden_layer_sizes=mlpcfg.get('hidden_layer_sizes', (100,)),
                               activation=mlpcfg.get('activation', 'relu'),
                               solver=mlpcfg.get('solver', 'lbfgs'),
                               alpha=mlpcfg.get('alpha', 0.001),
                               batch_size=mlpcfg.get('batch_size', 'auto'),
                               learning_rate=mlpcfg.get('learning_rate', 'constant'),
                               power_t=mlpcfg.get('power_t', 0.5),
                               max_iter=mlpcfg.get('max_iter', 200))
            mlp.fit(self.X_train, self.y_train)
            self.algs_dict['mlp'] = {'algo': mlp,
                                     'type': 'multi-layer perceptron regressor',
                                     'metric': self.metric}

    def predict_algos(self):
        # Make predictions with trained algorithms     
        for k, v in self.algs_dict.items():
            algo = v['algo']
            if self.verbose:
                print(f"      Generating predictions for {algo} algorithm.")   
            y_pred = algo.predict(self.X_test)
            self.preds_dict[k] = {'y_pred': y_pred,
                             'type': v['type'],
                             'metric': v['metric']}
        return self.preds_dict

    def evaluate_algos(self):
        # Evaluate the predictions
        if self.verbose:
            print(f"      Evaluating predictions.")   
        # TODO add more evaluation metrics here
        for k, v in self.preds_dict.items():
            y_pred = v['y_pred']
            self.eval_dict[k] = {'type': v['type'],
                            'metric': v['metric'],
                            'mse': mean_squared_error(self.y_test, y_pred),
                            'r2': r2_score(self.y_test, y_pred)}
        return self.eval_dict

    def save_algos(self):
        # Write algorithm to file & record save path in algs_dict['loc_algo']
        for algo in self.algs_dict.keys():
            if self.verbose:
                print(f"      Saving {algo} for {self.metric} to file")
            basename_alg_ds_metr = f'algo_{algo}_{self.metric}__{self.dataset_id}'
            path_algo = Path(self.dir_out_alg_ds) / Path(basename_alg_ds_metr + '.joblib')
            # write trained algorithm
            joblib.dump(self.algs_dict[algo]['algo'], path_algo)
            self.algs_dict[algo]['loc_algo'] = str(path_algo)
   
    def org_metadata_alg(self):
        # Must be called after running AlgoTrainEval.save_algos()
        # Record location of trained algorithm
        self.eval_df = pd.DataFrame(self.eval_dict).transpose().rename_axis(index='algorithm')

        self.eval_df['dataset'] = self.dataset_id

        # Assign the locations where algorithms were saved
        self.eval_df['loc_algo'] = [self.algs_dict[alg]['loc_algo'] for alg in self.algs_dict.keys()] 
        self.eval_df['algo'] = self.eval_df.index
        self.eval_df = self.eval_df.reset_index()
    
    def train_eval(self):
        # Overarching train, test, evaluation wrapper

        # Run the train/test split
        self.split_data()

        # Train algorithms # returns self.algs_dict 
        self.train_algos()

        # Make predictions  # 
        self.predict_algos()

        # Evaluate predictions # returns self.eval_dict
        self.evaluate_algos()

        # Write algorithms to file # returns self.algs_dict_paths
        self.save_algos()

        # Generate metadata dataframe
        self.org_metadata_alg() # Must be called after save_algos()
        
# %%
