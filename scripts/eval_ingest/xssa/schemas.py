import pandera as pa
from pandera import Column, DataFrameSchema, Index, Check
from typing import Any, Dict, Optional, Tuple
from pydantic import BaseModel, field_validator, model_validator
import numpy as np
import re
import yaml
from pathlib import Path

# %% Get data_source values
# Path to your YAML file
yaml_path = Path(__file__).resolve().parents[3] / "pkg" / "proc.attr.hydfab" / "inst" / "extdata" / "attr_source_types.yml"

with open(yaml_path, "r") as f:
    raw_sources = yaml.safe_load(f)

# Extract internal_dataset_name values
data_source_values = [
    d.get("internal_dataset_name")
    for v in raw_sources.values()
    for d in v if isinstance(d, dict) and "internal_dataset_name" in d
]
data_source_values = [v for v in data_source_values if v]  # Remove None

    # %% Introducing DataFrameSchema for dataframe objects
    # These could be validated further using fs_attr_menu.yaml and attr_source.type.yaml file
    # Inside the R package, proc.attr.hydfab
schema_df_attr = DataFrameSchema({
        "featureID": Column(pa.Object, nullable=False),  # accepts int or str
        "featureSource": Column(str,checks=pa.Check.isin(["COMID", "custom_hfuid"]),nullable=False),
        "data_source": Column(str,checks=pa.Check.isin(data_source_values),nullable=False),
        # "data_source": Column(str,checks=pa.Check.isin(["hydroatlas__v1", "usgs_nhdplus__v2"]),nullable=False),
        "dl_timestamp": Column(pa.DateTime,nullable=False),
        "attribute": Column(str,checks=pa.Check.str_length(1, 30),nullable=False),
        "value": Column(float,nullable=False),
    },
    index=Index(int, name=None),coerce=True,strict=True,name="DFAttr"
)


wkt_point_pattern = r"^POINT\s*\(\-?\d+(\.\d+)?\s+\-?\d+(\.\d+)?\)$"    
schema_gdf_comid = DataFrameSchema({
        "comid": Column(int, nullable=False),
        "gage_id": Column(int, nullable=False),
        "geometry": Column(str,checks=pa.Check.str_matches(wkt_point_pattern),nullable=False)
    },
    index=Index(int),coerce=True,strict=True,name="GDFComid"
)


schema_rslt_eval_df = pa.DataFrameSchema({
        "algorithm": Column(pa.String,checks=Check.isin(["rf", "mlp"]),nullable=False),
        "type": Column(pa.String,checks=Check.isin(["random forest regressor", "multi-layer perceptron regressor"]),nullable=False),
        "metric": Column(pa.String,checks=Check.isin(["NSE", "RMSE", "KGE"]),nullable=False),
        "mse": Column(pa.Float,nullable=False),
        "r2": Column(pa.Float,nullable=False),
        "dataset": Column(pa.String,nullable=False),
        "file_pipe": Column(pa.String,checks=Check.str_matches(r".+\.joblib$"),nullable=False),
        "algo": Column(pa.String,checks=Check.isin(["rf", "mlp"]),nullable=False),
    },
    index=pa.Index(pa.Int),coerce=True,strict=True,name="RsltEvalDF"
)

schema_attrs_sel = DataFrameSchema({
        0: Column(pa.String,nullable=False),
        },
    index=pa.Index(pa.Int),coerce=True,strict=True,name="AttrsSelDF"
)

schema_dat_resp = pa.DataFrameSchema({
    "basin_name": Column(str, nullable=False),
    "RMSE": Column(float, nullable=False),
    "NSE": Column(float, nullable=False),
    "KGE": Column(float, nullable=False),
    "gage_id": Column(int, nullable=False),
    "comid": Column(int, nullable=False),
})


def build_schema_df_pred(schema_df_pred_dict):
    return pa.DataFrameSchema(
        schema_df_pred_dict,
        index=pa.Index(pa.Int),
        coerce=True,
        strict=True,
        name="DFPred"
    )

