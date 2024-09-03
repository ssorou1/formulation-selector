#' @title Test case to use for algorithm prediction
#' @author Guy Litt
#' @description Selects a small sample to US locations from the Julie Mai xSSA
#' dataset and generates attributes to use for testing algo prediction capabilities
#' @reference https://www.nature.com/articles/s41467-022-28010-7

# Read in xssa raw data:
library(dplyr)
library(glue)
library(tidyr)
library(yaml)


home_dir <- Sys.getenv("HOME")

# USER INPUT: Paths to relevant config files
path_attr_config <- glue::glue("{home_dir}/git/formulation-selector/scripts/eval_ingest/xssa/xssa_attr_config.yaml")
path_raw_config <- glue::glue("{home_dir}/git/formulation-selector/scripts/eval_ingest/xssa/xssa_config.yaml")

# TODO specify a standardized file prediction name in a prediction config file

# ------------------------ ATTRIBUTE CONFIGURATION --------------------------- #
cfig_attr <- yaml::read_yaml(path_attr_config)

hfab_cfg <- cfig_attr[['hydfab_config']]
attr_sel_cfg <- cfig_attr[['attr_select']]
s3_base <- hfab_cfg[[base::grep("s3_base",hfab_cfg)]]$s3_base # s3 path containing hydrofabric-formatted attribute datasets
s3_bucket <- hfab_cfg[[base::grep("s3_bucket",hfab_cfg)]]$s3_bucket # s3 bucket containing hydrofabric data
s3_path_hydatl <- glue::glue(attr_sel_cfg[[base::grep("s3_path_hydatl",attr_sel_cfg)]]$s3_path_hydatl) # path to hydroatlas data formatted for hydrofabric

form_cfig <- cfig_attr[['formulation_metadata']]
datasets <- ds <- form_cfig[[grep("datasets",form_cfig)]]$datasets

dir_base <- file.path(home_dir,'noaa','regionalization','data')

io_cfig <- cfig_attr[['file_io']]

dir_base <- glue::glue(base::unlist(io_cfig)[['dir_base']])
dir_std_base <- glue::glue(base::unlist(io_cfig)[['dir_std_base']])
dir_db_hydfab <- glue::glue(base::unlist(io_cfig)[['dir_db_hydfab']])
dir_db_attrs <- glue::glue(base::unlist(io_cfig)[['dir_db_attrs']])


# Additional config options
hf_cat_sel <- base::unlist(hfab_cfg)[['hf_cat_sel']]#c("total","all")[1] # total: interested in the single location's aggregated catchment data; all: all subcatchments of interest
ext <- base::unlist(hfab_cfg)[['ext']]# 'gpkg'

for (x in attr_sel_cfg){
  print(names(x))
}

# The names of attribute datasets of interest (e.g. 'ha_vars', 'usgs_vars', etc.)
names_attr_sel <- base::lapply(attr_sel_cfg,
                               function(x) base::names(x)[[1]]) %>% unlist()

# Generate list of standard attribute dataset names containing sublist of variable IDs
ls_vars <- names_attr_sel[grep("_vars",names_attr_sel)]
vars_ls <- base::lapply(ls_vars, function(x) base::unlist(base::lapply(attr_sel_cfg, function(y) y[[x]])))
names(vars_ls) <- ls_vars


Retr_Params <- list(paths = list(# Note that if a path is provided, ensure the
  # name includes 'path'. Same for directory having variable name with 'dir'
  dir_db_hydfab=dir_db_hydfab,
  dir_db_attrs=dir_db_attrs,
  s3_path_hydatl = s3_path_hydatl,
  dir_std_base = dir_std_base),
  vars = vars_ls,
  datasets = datasets
)

# --------------------------- INPUT DATA READ -------------------------------- #
raw_cfg <- yaml::read_yaml(path_raw_config)

# Read in the xssa dataset, remove extraneous spaces, subselect USGS gages
path_data <- glue::glue(raw_cfg[['file_io']][[grep("path_data",raw_cfg[['file_io']])]]$path_data)
df_all_xssa <- utils::read.csv(path_data,sep = ';', colClasses=c("basin_id"="character"))

# Read in the CAMELS dataset so we can pick non-CAMELS locations for testing
path_camels <-  glue::glue(raw_cfg[['file_io']][[grep("path_camels",raw_cfg[['file_io']])]]$path_camels)
df_camels <- utils::read.csv(path_camels,sep=';',colClasses=c("gauge_id"="character"))

# --------------------------- INPUT DATA MUNGE ------------------------------- #
# Remove extraneous spaces, subselect USGS gages from the xssa dataset
df_all_xssa <- utils::read.csv(path_data,sep = ';', colClasses=c("basin_id"="character"))
df_all_xssa[['basin_id']] = base::gsub("\\ ","",df_all_xssa[['basin_id']])
df_all_xssa[['basin_id_num']] <- as.numeric(df_all_xssa[['basin_id']] )
df_us_xssa <- df_all_xssa %>% tidyr::drop_na() # Canadian gages have letters


non_intersect_xssa <- base::setdiff(df_us_xssa$basin_id, df_camels$gauge_id)
non_intersect_camels <- base::setdiff(df_camels$gauge_id,df_all_xssa$basin_id)

# Randomly sample non_intersecting, since this is just for testing
set.seed(432)
subsamp_locs <- base::sample(non_intersect_xssa,size=20)
df_subsamp_locs <- data.frame(gage_id = subsamp_locs)

# ---------------------- Grab all needed attributes ---------------------- #
# Now acquire the attributes:
ls_comids <- proc.attr.hydfab::proc_attr_gageids(gage_ids=subsamp_locs,
                                                 featureSource='nwissite',
                                                 featureID='USGS-{gage_id}',
                                                 Retr_Params,
                                                 lyrs=lyrs,
                                                 overwrite=overwrite)
df_pred <- base::data.frame('nwis_id' = names(ls_comids),
                 'comid' = unlist(ls_comids))

# TODO specify a standardized file prediction name in a config file
save_path_pred <- file.path(dir_std_base,"prediction_locations_comid.csv")
write.csv(df_pred,file = save_path_pred,row.names = FALSE)
