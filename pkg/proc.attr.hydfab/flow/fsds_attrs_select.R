#' @title Script to quality control queries of available attributes
#' @author Lauren Bolotin \code{lauren.bolotin@noaa.gov}
#' @description
#' Provided a *_attr_config.yaml file, check that the user-specified attributes
#' exist in compatible attribute datasets so they can be subset and grabbed. 
#' @details 
#'

#' @seealso [fsds_proc] A python package that processes input data for the
#' formulation-selector

# Changelog / Contributions
#   2024-08-07 Originally created, LB

library(yaml)

# Read in the menu of attributes available through FSDS
attr_menu <- yaml::read_yaml('./fsds_attr_menu_reformat.yaml')
# attr_menu <- yaml::read_yaml('./fsds_attr_menu.yaml')
# attr_menu <- yaml::read_yaml('./fsds_attr_menu_nocat.yaml')

# 
# idxs_vars <- base::lapply(attr_menu$hydroatlas_attributes, function(x) names(x)) %>%
#   base::unlist()
# 
# ls_vars <- base::sapply(idxs_vars, function(x) raw_config$attr_select[[x]])

# test <- unlist(attr_menu$hydroatlas_attributes$hydrology) # pulls pair of var name and description
test <- names(attr_menu$hydroatlas_attributes$hydrology) # pulls var name only

# Read in the user defined config of attributes of interest
# attr_cfg <- yaml::read_yaml('../../scripts/eval_ingest/xssa/xssa_attr_config_reformat2.yaml')
attr_cfg <- yaml::read_yaml('../../scripts/eval_ingest/xssa/xssa_attr_config.yaml')

# TODO: I REALLY suggest reformatting the YAML files so no indexing by numbers is necessary. 
# This can be done by removing the bullet points " - " in each line and relying only on indentation.
# That way this workflow will not rely on things being in a particular order
# Determine which data sets the user specifies attributes from
ha <- attr_cfg[["attr_select"]][[1]][["ha_vars"]]
ha_sel <- base::ifelse((base::length(ha) > 0) & !base::is.null(ha[1][[1]]), 
                 TRUE, FALSE)
usgs <- attr_cfg[["attr_select"]][[2]][["usgs_vars"]]
usgs <- base::ifelse((base::length(usgs) > 0) & !base::is.null(usgs[1][[1]]), 
               TRUE, FALSE)
sc <- attr_cfg[["attr_select"]][[3]][["sc_vars"]]
sc <- base::ifelse((base::length(sc) > 0) & !base::is.null(sc[1][[1]]),
             TRUE, FALSE)

# Check if the entered variables exist in the attribute menu
ha_menu <- base::tolower(attr_menu$hydroatlas_attributes)

attr_select <- attr_cfg$attr_select

ha_sel <- base::tolower(attr_select[[1]]$ha_vars)
usgs_sel <- base::tolower(attr_select[[2]]$usgs_vars)
sc_sel <- base::tolower(attr_select[[3]]$sc_vars)

ha_var_menu <- base::names(base::unlist(attr_menu$hydroatlas_attributes))
camels_var_menu <- base::names(base::unlist(attr_menu$camels_attributes))

