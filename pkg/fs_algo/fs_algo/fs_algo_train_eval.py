from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler, FunctionTransformer
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import GridSearchCV
import numpy as np
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
import forestci as fci

# %% BASIN ATTRIBUTES (PREDICTORS) & RESPONSE VARIABLES (e.g. METRICS)
class AttrConfigAndVars:
    def __init__(self, path_attr_config: str | os.PathLike):
        self.path_attr_config = path_attr_config
        self.attrs_cfg_dict = dict()
        self.attr_config = dict()

    def _read_attr_config(self ) -> dict:
        """Extract the desired basin attribute variable names from yaml file

        :raises warnings.warn: Assumes all attributes desired if not specified
        :return: dictionary of required configuration items: 
            - `attrs_sel`: attributes `list[str]`
            - `dir_db_attrs`: directory where attribute .parquet files live `list[str]`
            - `dir_std_base`: directory of standardized data generated by :mod:`fsds_proc`, `list[str]`
            - `dir_base`: base directory for file output, `list[str]`
            - `datasets`: dataset names, `list[str]`
        :rtype: dict
        """

        # Attribute data location:
        with open(self.path_attr_config, 'r') as file:
            self.attr_config = yaml.safe_load(file)

        # identify attribute data of interest from attr_config
        attrs_all = [v for x in self.attr_config['attr_select'] for k,v in x.items() if '_vars' in k]
        attrs_sel = [x for x in list(itertools.chain(*attrs_all)) if x is not None]

        if len(attrs_sel) == None: # If no attributes generated, assume all attributes are of interest
            attrs_sel = 'all'
            raise warnings.warn(f"No attributes discerned from 'attr_select'. Assuming all attributes desired.",UserWarning)
        
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


def fs_read_attr_comid(dir_db_attrs:str | os.PathLike, comids_resp:list | Iterable, attrs_sel: str | Iterable = 'all',
                       _s3 = None,storage_options=None)-> pd.DataFrame:
    """Read attribute data acquired using fsds.attr.hydfab R package & subset to desired attributes

    :param dir_db_attrs: directory where attribute .parquet files live
    :type dir_db_attrs: str | os.PathLike
    :param comids_resp: USGS COMID values of interest 
    :type comids_resp: list | Iterable
    :param attrs_sel: desired attributes to select from the attributes .parquet files, defaults to 'all'
    :type attrs_sel: str | Iterable, optional
    :param _s3: future feature, defaults to None
    :type _s3: future feature, optional
    :param storage_options: future feature, defaults to None
    :type storage_options: future feature, optional
    :return: dict of the following keys:
        - `attrs_sel`
        - `dir_db_attrs`
        - `dir_std_base`
        - `dir_base`
        - `datasets`
    :rtype: pd.DataFrame
    """
    if _s3:
        storage_options={"anon",True} # for public
        # TODO  Setup the s3fs filesystem that will be used, with xarray to open the parquet files
        #_s3 = s3fs.S3FileSystem(anon=True)

    # Read attribute data acquired using fsds.attr.hydfab R package
    all_attr_ddf = dd.read_parquet(dir_db_attrs, storage_options = storage_options)

    # Subset based on comids of interest
    attr_ddf_subloc = all_attr_ddf[all_attr_ddf['featureID'].str.contains('|'.join(comids_resp))]

    if attr_ddf_subloc.shape[0].compute() == 0:
        warnings.warn(f'None of the provided featureIDs exist in {dir_db_attrs}: \
                      \n {", ".join(attrs_sel)} ', UserWarning)
    
    # Subset based on attributes of interest
    if attrs_sel == 'all':
        attrs_sel = attr_ddf_subloc['attribute'].unique().compute()

    attr_ddf_sub = attr_ddf_subloc[attr_ddf_subloc['attribute'].str.contains('|'.join(attrs_sel))]
    
    attr_df_sub = attr_ddf_sub.compute()

    if attr_df_sub.shape[0] == 0:
        warnings.warn(f'The provided attributes do not exist with the retrieved featureIDs : \
                        \n {",".join(attrs_sel)}',UserWarning)
 
    # Run check that all variables are present across all basins
    dict_rslt = _check_attributes_exist(attr_df_sub,attrs_sel)
    attr_df_sub, attrs_sel_ser = dict_rslt['df_attr'], dict_rslt['attrs_sel']

    if not pd.api.types.is_float_dtype(attr_df_sub['value']):
        warnings.warn("Forcing all attribute values to be float")
        attr_df_sub['value'] = np.float64(attr_df_sub['value'])

    if attr_df_sub['value'].isna().any():
        warnings.warn('The attribute dataset contains unexpected NA values, \
                      which may be problematic for some algo training/testing. \
                      \nConsider reprocessing the attribute grabber (fsds.attr.hydfab R package)',
                      UserWarning)

    return attr_df_sub

def _check_attributes_exist(df_attr: pd.DataFrame, attrs_sel:pd.Series | Iterable) -> Dict[pd.DataFrame, pd.Series]:
    """ Checks if any COMIDs have different numbers of attributes. It's expected that they all have the same attributes.

    :param df_attr: The attribute data, as generated in :func:`fs_read_attr_comid()`
    :type df_attr: pd.DataFrame
    :param attrs_sel: the names of the attributes
    :type attrs_sel: pd.Series | Iterable
    :return: the same objects df_attr, and attrs_sel, but attrs_sel is ensured to be a pd.Series
    :rtype: Dict[pd.DataFrame, pd.Series]
    :seealso: :func:`fs_read_attr_comid()`

    """
    #
    if not isinstance(attrs_sel,pd.Series):
            # Convert to a series for convenience of pd.Series.isin()
            attrs_sel = pd.Series(attrs_sel)

    # Run check that all attributes are present for all basins
    if df_attr.groupby('featureID')['attribute'].count().nunique() != 1:
        
        # multiple combos of comid/attrs exist. Find them and warn about it.
        vec_missing = df_attr.groupby('featureID')['attribute'].count() != len(attrs_sel)
        bad_comids = vec_missing.index.values[vec_missing]
        
        warnings.warn(f"    TOTAL unique locations with missing attributes: {len(bad_comids)}",UserWarning)
        df_attr_sub_missing = df_attr[df_attr['featureID'].isin(bad_comids)]
    
        missing_attrs = attrs_sel[~attrs_sel.isin(df_attr_sub_missing['attribute'])]
        warnings.warn(f"    TOTAL MISSING ATTRS: {len(missing_attrs)}",UserWarning)
        str_missing = '\n    '.join(missing_attrs.values)

        warn_msg_missing_attrs = "\
        \n Not all featureID groupings (i.e. COMID groups) contain the same number of catchment attributes. \
        \n This could be problematic for model training. \
        \n Consider running attribute grabber with fsds.attr.hydfab."
        warn_msg2 = "\nMissing attributes include: \n    " + str_missing
        warn_msg_3 = "\n COMIDs with missing attributes include: \n" + ', '.join(bad_comids)
        warnings.warn(warn_msg_missing_attrs + warn_msg2 + warn_msg_3,UserWarning)
        
    
    return {'df_attr': df_attr, 'attrs_sel': attrs_sel}

def _find_feat_srce_id(dat_resp: Optional[xr.core.dataset.Dataset] = None,
                       attr_config: Optional[Dict] = None) -> List[str]:
    """ Try grabbing :mod:`fsds_proc` standardized dataset attributes &/or config file.

    :param dat_resp: The standardized dataset generated by :mod:`fsds_proc`, defaults to None
    :type dat_resp: Optional[xr.core.dataset.Dataset], optional
    :param attr_config: configuration data generated from the attribute configuration file, defaults to None
    :type attr_config: Optional[Dict], optional
    :raises ValueError: featureSource could not be identified from the provided
    :raises ValueError: _description_
    :return: _description_
    :rtype: List[str]

    note:: Standardized dataset attributes preferred in cases of processing multiple datasets & attributes differ by dataset). 
    Otherwise, fallback on config file. At least one argument must be provided.
    """
    
    featureSource = None
    try: # dataset attributes first
        featureSource = dat_resp.attrs.get('featureSource', None)
    except (KeyError, StopIteration,AttributeError): # config file second
        pass
    if featureSource is None:
        try: 
            featureSource = next(x['featureSource'] for x in attr_config['col_schema'] if 'featureSource' in x)
        except:
            pass
    
    if not featureSource:
        raise ValueError(f'The featureSource could not be found. Ensure it is present in the col_schema section of the attribute config file.')
    # Attempt to grab featureID from dataset attributes, fallback to the config file
    featureID = None
    try: # dataset attributes first
        featureID = dat_resp.attrs.get('featureID', None)
    except (KeyError, StopIteration,AttributeError): # config file second
        pass
    if featureID is None:
        try:
            featureID = next(x['featureID'] for x in attr_config['col_schema'] if 'featureID' in x)
        except:
            pass
    if not featureID:
        raise ValueError(f'The featureID could not be found. Ensure it is present in the col_schema section of the attribute config file.')
        # TODO need to map gage_id to location identifier in attribute data!

    return [featureSource, featureID]

def fs_retr_nhdp_comids(featureSource:str,featureID:str,gage_ids: Iterable[str] ) ->list:    
    """Retrieve response variable's comids, querying the shortest distance in the flowline

    :param featureSource: the datasource for featureID from the R function :mod:`nhdplusTools` :func:`get_nldi_features()`, e.g. 'nwissite'
    :type featureSource: str
    :param featureID: The conversion format of `gage_ids` into a recognizable string for :mod:`nhdplusTools`, which is an f-string configured conversion of `gage_id` e.g. `'USGS-{gage_id}'`. Expected to contain the string `"{gage_id}"`
    :type featureID: str
    :param gage_ids: The location identifiers compatible with the format specified in `featureID`
    :type gage_ids: Iterable[str]
    :raises warnings.warn: In case number of retrieved comids does not match total requested gage ids
    :return: The COMIDs corresponding to the provided location identifiers, `gage_ids`
    :rtype: list
    """

    nldi = nhd.NLDI()
    comids_resp = [nldi.navigate_byid(fsource=featureSource,fid= featureID.format(gage_id=gage_id),
                                navigation='upstreamMain',
                                source='flowlines',
                                distance=1 # the shortest distance
                                ).loc[0]['nhdplus_comid'] 
                                for gage_id in gage_ids]
    
    if len(comids_resp) != len(gage_ids) or comids_resp.count(None) > 0: # May not be an important check
        raise warnings.warn("The total number of retrieved comids does not match \
                      total number of provided gage_ids",UserWarning)

    return comids_resp

def build_cfig_path(path_known_config:str | os.PathLike, path_or_name_cfig:str | os.PathLike) -> os.PathLike | None:
    """Build the expected configuration file path within the RAFTS framework

    :param path_known_config: path of the known configuration sent 
    :type path_known_config: str | os.PathLike
    :param path_or_name_cfig: Path or name of configuration file. If only name provided, it's assumed it resides in same directory as `path_known_config`
    :type path_or_name_cfig: str | os.PathLike
    :raises FileNotFoundError: The provided `path_known_config` does not exist
    :raises FileNotFoundError: The desired configuration file does not exist
    :return: The path to another relevant configuration file used for a different step in RAFTS processing
    :rtype: os.PathLike | None
    """
    dir_parent_cfig = Path(path_known_config).parent
    if not dir_parent_cfig.exists():
        raise FileNotFoundError(f"The provided 'known' configuration file does not exist: \n{path_known_config}")
    if path_or_name_cfig: # Only perform if path_or_name_cfig not None
        path_cfig = Path(dir_parent_cfig/Path(path_or_name_cfig))
        if not path_cfig.exists():
            path_cfig = Path(path_or_name_cfig)
            if not path_cfig.exists():
                raise FileNotFoundError(f'The following configuration file could not be found: \n{path_or_name_cfig}')
    else:
        warnings.warn("The configuration file may not have specified the path or file name.",UserWarning)
        path_cfig = None
    return path_cfig

def fs_save_algo_dir_struct(dir_base: str | os.PathLike ) -> dict:
    """Generate a standard file saving directory structure

    :param dir_base: The base directory for saving output
    :type dir_base: str | os.PathLike
    :raises ValueError: If the base directory does not exist
    :return: Full paths to the `output` and `trained_algorithms` directories
    :rtype: dict
    """

    if not Path(dir_base).exists():
        raise ValueError(f"The provided dir_base does not exist. \
                         \n Double check the config file to make sure \
                         \n an existing directory is provided. dir_base= \
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

def _open_response_data_fsds(dir_std_base: str | os.PathLike, ds:str) -> xr.Dataset:
    """Read in standardized dataset generated from :mod:`fsds_proc`

    :param dir_std_base: The directory containing the standardized dataset generated from `fsds_proc`
    :type dir_std_base: str | os.PathLike
    :param ds: a string that's unique to the dataset of interest, generally not containing the file extension. 
    There should be a netcdf .nc or zarr .zarr file containing matches to this string
    :type ds: str
    :raises ValueError: The directory where the dataset file should live does not exist.
    :raises ValueError: Could not successfully read in the dataset `ds` as a .nc or .zarr
    :return: The hierarchical dataset as an xarray, as generated by :mod:`fsds_proc`
    :rtype: xr.Dataset
    """
    # Implement a check to ensure each dataset directory exists
    if not Path(dir_std_base).exists:
        raise ValueError(f'The dir_std_base directory does not exist. Double check dir_std_base: \
                         \n{dir_std_base}')
    
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

def std_algo_path(dir_out_alg_ds:str | os.PathLike, algo: str, metric: str, dataset_id: str) -> str:
    """Standardize the algorithm save path
    :param dir_out_alg_ds:  Directory where algorithm's output stored.
    :type dir_out_alg_ds: str | os.PathLike
    :param algo: The type of algorithm
    :type algo: str
    :param metric:  The metric or hydrologic signature identifier of interest
    :type metric: str
    :param dataset_id: Unique identifier/descriptor of the dataset of interest
    :type dataset_id: str
    :return: full save path for joblib object
    :rtype: str
    """
    Path(dir_out_alg_ds).mkdir(exist_ok=True,parents=True)
    basename_alg_ds_metr = f'algo_{algo}_{metric}__{dataset_id}'
    path_algo = Path(dir_out_alg_ds) / Path(basename_alg_ds_metr + '.joblib')
    return path_algo

def std_pred_path(dir_out: str | os.PathLike, algo: str, metric: str, dataset_id: str) -> str:
    """Standardize the prediction results save path

    :param dir_out: The base directory for saving output
    :type dir_out: str | os.PathLike
    :param algo: The type of algorithm
    :type algo: str
    :param metric: The metric or hydrologic signature identifier of interest
    :type metric: str
    :param dataset_id: Unique identifier/descriptor of the dataset of interest
    :type dataset_id: str
    :return: full save path for parquet dataframe object of results
    :rtype: str
    """
    dir_preds_base = Path(Path(dir_out)/Path('algorithm_predictions'))
    dir_preds_ds = Path(dir_preds_base/Path(dataset_id))
    dir_preds_ds.mkdir(exist_ok=True,parents=True)
    basename_pred_alg_ds_metr = f"pred_{algo}_{metric}__{dataset_id}.parquet"
    path_pred_rslt = Path(dir_preds_ds)/Path(basename_pred_alg_ds_metr)
    return path_pred_rslt

def _read_pred_comid(path_pred_locs: str | os.PathLike, comid_pred_col:str ) -> list[str]:
    """Read the comids from a prediction file formatted as .csv

    :param path_pred_locs: The path to prediction data location, containing the comid
    :type path_pred_locs: str | os.PathLike
    :param comid_pred_col: The column name corresponding to the comid inside the prediction location dataset
    :type comid_pred_col: str
    :raises ValueError: Could not read the location data file and/or subselect the comid column
    :raises ValueError: File extension of location data file not recognized
    :return: list of comids
    :rtype: list[str]
    """
    if not Path(path_pred_locs).exists():
        FileNotFoundError(f"The path to prediction location data could not be found: \n{path_pred_locs} ")
    if '.csv' in Path(path_pred_locs).suffix:
        try:
            comids_pred = pd.read_csv(path_pred_locs)[comid_pred_col].values
        except:
            raise ValueError(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
    else:
        raise ValueError(f"NEED TO ADD CAPABILITY THAT HANDLES {Path(path_pred_locs).suffix} file extensions")
    comids_pred = [str(x) for x in comids_pred]
    return comids_pred
class AlgoTrainEval:
    def __init__(self, df: pd.DataFrame, attrs: Iterable[str], algo_config: dict,
                 dir_out_alg_ds: str | os.PathLike, dataset_id: str,
                 metr: str, test_size: float = 0.3,rs: int = 32,
                 verbose: bool = False):
        """The algorithm training and evaluation class.

        :param df: The combined response variable and predictor variables DataFrame.
        :type df: pd.DataFrame
        :param attrs: The column names of attributes.
        :type attrs: Iterable[str]
        :param algo_config: The algorithm configuration as read from the algo_config yaml where each key is the algorithm that will be run. Presently allowable keys include:
            - `rf`:  :class:`sklearn.ensemble.RandomForestRegressor` algorithm. Strongly recommended to include.
            - `mlp`:  :class:`sklearn.neural_network.MLPRegressor` multilayer perceptron algorithm.
            Each algorithm key contains sub-dict keys for the parameters that may be passed to the corresponding :mod:`sklearn` algorithm. 
            If no parameters keys are passed, the :mod:`sklearn` algorithm's default arguments are used.
        :type algo_config: dict
        :param dir_out_alg_ds: Directory where algorithm's output stored.
        :type dir_out_alg_ds: str | os.PathLike
        :param dataset_id: Unique identifier/descriptor of the dataset of interest, and will be used in file writing.
        :type dataset_id: str
        :param metr: Column name in `df`. The metric or hydrologic signature identifier of interest, defaults to None.
        :type metr: str, optional
        :param test_size: Parameter for :func:`sklearn.model_selection.train_test_split`, represents proportion of dataset to include in the test split, defaults to 0.3.
        :type test_size: float, optional
        :param rs: The random seed, defaults to 32.
        :type rs: int, optional
        :param verbose: Should print, defaults to False.
        :type verbose: bool, optional
        """
        # class args
        self.df = df
        self.attrs = attrs
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
        
        # grid search
        self.algo_config_grid = dict()
        self.grid_search_algs = list()

        # train/pred/eval metadata
        self.algs_dict = {}
        self.preds_dict = {}
        self.eval_dict = {}

        # The evaluation summary result
        self.eval_df = pd.DataFrame()

    
    def split_data(self):
        """Split dataframe into training and testing predictors (X) and response (y) variables using :func:`sklearn.model_selection.train_test_split`

        """
        
        if self.verbose:
            print(f"      Performing train/test split as {round(1-self.test_size,2)}/{self.test_size}")

        # Check for NA values first
        self.df_non_na = self.df[self.attrs + [self.metric]].dropna()
        if self.df_non_na.shape[0] < self.df.shape[0]:
            warnings.warn(f"\
                \n   !!!!!!!!!!!!!!!!!!!\
                \n   NA VALUES FOUND IN INPUT DATASET!! \
                \n   DROPPING {self.df.shape[0] - self.df_non_na.shape[0]} ROWS OF DATA. \
                \n   !!!!!!!!!!!!!!!!!!!",UserWarning)
            

        X = self.df_non_na[self.attrs]
        y = self.df_non_na[self.metric]
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(X,y, test_size=self.test_size, random_state=self.rs)

    
    def convert_to_list(self,d:dict) ->dict:
        """Runcheck: In situations where self.algo_config_grid is used, all objects must be iterables 

        :param d: A dict containing sub-dicts with key-value pairs
        :type d: dict
        :return: The dict where any non-iterable values have been converted into a list
        :rtype: dict
        """
        for key, value in d.items():
            if isinstance(value, dict):
                self.convert_to_list(value)
            elif not isinstance(value, (list, tuple)):
                d[key] = [value]
        return(d)

    def list_to_dict(self, config_ls):
        # When a config object is inconveniently formatted as a list of multiple dict
        if isinstance(config_ls,list):
            config_dict = {}
            for d in config_ls:
                config_dict.update(d)
        else:
            config_dict = config_ls
        return config_dict
    
    def select_algs_grid_search(self):
        """Determines which algorithms' params involve hyperparameter tuning
        """
        ls_move_to_srch_cfig = list()
        for k, alg_ls in self.algo_config.items():
            sub_dict = {k: alg_dict[k] for alg_dict in alg_ls for k in alg_dict.keys()}
            totl_opts_per_param = list()
            for kk, v in sub_dict.items():
                if isinstance(v,Iterable) and len(v)>1:
                    totl_opts_per_param.append(len(v))
                else:
                    totl_opts_per_param.append(1)
            if any([x > 1 for x in totl_opts_per_param]):
                if self.verbose:
                    print(f"Performing grid search CV for {k}")
                self.grid_search_algs.append(k)
                ls_move_to_srch_cfig.append(k)

        # Move hyperparams from basic algo params into grid search params
        for k in ls_move_to_srch_cfig:
            self.algo_config_grid[k] = self.algo_config.pop(k)
        
        # Convert lists inside algo_config_grid['algo_name_here'] to a dict:
        dict_acg = {}
        for key, val in self.algo_config_grid.items():
            dict_acg[key] = self.list_to_dict(val)
        self.algo_config_grid = dict_acg

        # Convert lists inside algo_config['algo_name_here'] to a dict:    
        dict_ac = {}
        for key, val in self.algo_config.items():
            dict_ac[key] = self.list_to_dict(val)
        self.algo_config = dict_ac

        if self.algo_config_grid: # If there are non iterable values, convert them to lists to aid the algo training
            # e.g. {'activation':'relu'} becomes {'activation':['relu']}
            self.algo_config_grid  = self.convert_to_list(self.algo_config_grid)

    def train_algos(self):
        """Train algorithms based on what has been defined in the algo config file Algorithm options include the following:
        
            - `rf` for :class:`sklearn.ensemble.RandomForestRegressor`
            - `mlp` for :class:`sklearn.neural_network.MLPRegressor`
        """
        # Train algorithms based on config
        if 'rf' in self.algo_config:  # RANDOM FOREST
            if self.verbose:
                print(f"      Performing Random Forest Training")
            
            rf = RandomForestRegressor(n_estimators=self.algo_config['rf'].get('n_estimators'),
                                       oob_score=True,
                                       random_state=self.rs,
                                       )
            # --- Inserting forestci for uncertainty calculation ---
            ci = fci.random_forest_error(
                forest=rf,
                X_train_shape=self.X_train.shape,
                X_test=self.X_test,  # Assuming X contains test samples
                inbag=None, 
                calibrate=True, 
                memory_constrained=False, 
                memory_limit=None, 
                y_output=0  # Change this if multi-output
            )
            # ci now contains the confidence intervals for each prediction
            pipe_rf = make_pipeline(rf)                           
            pipe_rf.fit(self.X_train, self.y_train)
            self.algs_dict['rf'] = {'algo': rf,
                                    'pipeline': pipe_rf,
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
            pipe_mlp = make_pipeline(StandardScaler(),mlp)
            pipe_mlp.fit(self.X_train, self.y_train)
            self.algs_dict['mlp'] = {'algo': mlp,
                                     'pipeline': pipe_mlp,
                                     'type': 'multi-layer perceptron regressor',
                                     'metric': self.metric}

   
    def train_algos_grid_search(self):
        """Train algorithms using GridSearchCV based on the algo config file.
        
        Algorithm options include the following:
        
            - `rf` for :class:`sklearn.ensemble.RandomForestRegressor`
            - `mlp` for :class:`sklearn.neural_network.MLPRegressor`
        """

        if 'rf' in self.algo_config_grid:  # RANDOM FOREST
            if self.verbose:
                print(f"      Performing Random Forest Training with Grid Search")
            rf = RandomForestRegressor(oob_score=True, random_state=self.rs)
            # TODO move into main Param dict
            param_grid_rf = {
                'randomforestregressor__n_estimators': self.algo_config_grid['rf'].get('n_estimators', [100, 200, 300])
            }
            pipe_rf = make_pipeline(rf)
            grid_rf = GridSearchCV(pipe_rf, param_grid_rf, cv=5, scoring='neg_mean_absolute_error', n_jobs=-1)
            grid_rf.fit(self.X_train, self.y_train)
            self.algs_dict['rf'] = {'algo': grid_rf.best_estimator_.named_steps['randomforestregressor'],
                                    'pipeline': grid_rf.best_estimator_,
                                    'gridsearchcv': grid_rf,
                                    'type': 'random forest regressor',
                                    'metric': self.metric}

        if 'mlp' in self.algo_config_grid:  # MULTI-LAYER PERCEPTRON
            if self.verbose:
                print(f"      Performing Multilayer Perceptron Training with Grid Search")
            mlpcfg = self.algo_config_grid['mlp']
            mlp = MLPRegressor(random_state=self.rs)
            param_grid_mlp = {
                'mlpregressor__hidden_layer_sizes': mlpcfg.get('hidden_layer_sizes', [(100,), (50, 50)]),
                'mlpregressor__activation': mlpcfg.get('activation', ['relu', 'tanh']),
                'mlpregressor__solver': mlpcfg.get('solver', ['lbfgs', 'adam']),
                'mlpregressor__alpha': mlpcfg.get('alpha', [0.001, 0.01]),
                'mlpregressor__learning_rate': mlpcfg.get('learning_rate', ['constant', 'adaptive']),
                'mlpregressor__max_iter': mlpcfg.get('max_iter', [200, 300])
            }
            pipe_mlp = make_pipeline(StandardScaler(), mlp)
            grid_mlp = GridSearchCV(pipe_mlp, param_grid_mlp, cv=5, scoring='neg_mean_absolute_error', n_jobs=-1)
            grid_mlp.fit(self.X_train, self.y_train)
            self.algs_dict['mlp'] = {'algo': grid_mlp.best_estimator_,
                                    'pipeline': grid_mlp,
                                    'type': 'multi-layer perceptron regressor',
                                    'metric': self.metric}

    def predict_algos(self) -> dict:
        """ Make predictions with trained algorithms   

        :return: Evaluation results, with the following keys:
            - `y_pred`: The predicted values vector
            - `type`: The type of algorithm used for prediction (e.g. `"random forest"`)
            - `metric`: The formulation evaluation metric or hydrologic signature represented by `y_pred`
        :rtype: dict
        """
          
        for k, v in self.algs_dict.items():
            algo = v['algo']
            pipe = v['pipeline']
            type_algo = v['type']
            if self.verbose:
                print(f"      Generating predictions for {type_algo} algorithm.")   
            
            y_pred = pipe.predict(self.X_test)
            self.preds_dict[k] = {'y_pred': y_pred,
                             'type': v['type'],
                             'metric': v['metric']}
        return self.preds_dict

    def evaluate_algos(self) -> dict:
        """ Evaluate the predictions

        :return: Evaluation of algorithm performance from the test dataset. The dict keys include the following:
            - `type`: They type of algorithm, e.g. "random forest"
            - `metric`: the formulation metric, or hydrologic signature being predicted
            - `mse`: algorithm's mean squared error from test data
            - `r2`: algorithm's r-squared from test data
        :rtype: dict
        """
       
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
        """ Write pipeline to file & record save path in `algs_dict['loc_pipe']`

        """
        
        for algo in self.algs_dict.keys():
            if self.verbose:
                print(f"      Saving {algo} pipeline for {self.metric} to file")

            path_algo = std_algo_path(self.dir_out_alg_ds, algo, self.metric, self.dataset_id)
            # basename_alg_ds_metr = f'algo_{algo}_{self.metric}__{self.dataset_id}'
            # path_algo = Path(self.dir_out_alg_ds) / Path(basename_alg_ds_metr + '.joblib')
            # write trained algorithm
            joblib.dump(self.algs_dict[algo]['pipeline'], path_algo)
            self.algs_dict[algo]['loc_pipe'] = str(path_algo)
   
    def org_metadata_alg(self):
        """Must be called after running AlgoTrainEval.save_algos(). Records saved location of trained algorithm

        """

        self.eval_df = pd.DataFrame(self.eval_dict).transpose().rename_axis(index='algorithm')

        self.eval_df['dataset'] = self.dataset_id

        # Assign the locations where algorithms were saved
        self.eval_df['loc_pipe'] = [self.algs_dict[alg]['loc_pipe'] for alg in self.algs_dict.keys()] 
        self.eval_df['algo'] = self.eval_df.index
        self.eval_df = self.eval_df.reset_index()
    
    def train_eval(self):
        """ The overarching train, test, evaluation wrapper that also saves algorithms and evaluation results

        """

        # Run the train/test split
        self.split_data()

        # Check whether supplied params designed for grid search:
        self.select_algs_grid_search()

        # Train algorithms; returns self.algs_dict 
        if self.grid_search_algs: # Perform hyperparameterization grid search for these algos
            self.train_algos_grid_search()

        if self.algo_config: # Just run a single simulation for these algos
            self.train_algos()

        # Make predictions  # 
        self.predict_algos()

        # Evaluate predictions; returns self.eval_dict
        self.evaluate_algos()

        # Write algorithms to file; returns self.algs_dict_paths
        self.save_algos()

        # Generate metadata dataframe
        self.org_metadata_alg() # Must be called after save_algos()
        
