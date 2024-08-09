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
# attr_menu <- yaml::read_yaml('./fsds_attr_menu.yaml')
attr_menu <- yaml::read_yaml('./fsds_attr_menu_nocat.yaml')


# test <- unlist(attr_menu$hydroatlas_attributes$hydrology) # pulls pair of var name and description
# test <- names(attr_menu$hydroatlas_attributes$hydrology) # pulls var name only

# Read in the user defined config of attributes of interest
# attr_cfg <- yaml::read_yaml('../../scripts/eval_ingest/xssa/xssa_attr_config_reformat2.yaml')
attr_cfg <- yaml::read_yaml('../../scripts/eval_ingest/xssa/xssa_attr_config.yaml')

# Determine which data sets the user specifies attributes from
ha_vars_sel <- base::lapply(attr_cfg$attr_select, function(x) names(x)) %>%
  base::unlist() %>% base::grep(pattern = "ha_vars")
ha_vars_sel <- base::sapply(ha_vars_sel, function(x) attr_cfg$attr_select[[x]]) |> 
  unlist() |> 
  unname() |>
  tolower()

usgs_vars_sel <- base::lapply(attr_cfg$attr_select, function(x) names(x)) %>%
  base::unlist() %>% base::grep(pattern = "usgs_vars")
usgs_vars_sel <- base::sapply(usgs_vars_sel, function(x) attr_cfg$attr_select[[x]]) |> 
  unlist() |> 
  unname() |>
  tolower()

sc_vars_sel <- base::lapply(attr_cfg$attr_select, function(x) names(x)) %>%
  base::unlist() %>% base::grep(pattern = "sc_vars")
sc_vars_sel <- base::sapply(sc_vars_sel, function(x) attr_cfg$attr_select[[x]]) |> 
  unlist() |> 
  unname() |>
  tolower()

vars_sel <- c(ha_vars_sel, usgs_vars_sel, sc_vars_sel) # camels_vars_sel
rm(ha_vars_sel, usgs_vars_sel, sc_vars_sel, camels_vars_sel)

# Check if the entered variables exist in the attribute menu
ha_menu <- unlist(attr_menu$hydroatlas_attributes) |> names() |> tolower()
# usgs_menu <- unlist(attr_menu$usgs_attributes) |> names() |> tolower()
# sc_menu <- unlist(attr_menu$sc_attributes) |> names() |> tolower()
camels_menu <- unlist(attr_menu$camels_attributes) |> names() |> tolower()
vars_menu <- c(ha_menu, camels_menu) # sc_menu, usgs_menu
rm(ha_menu, usgs_menu, sc_menu, camels_menu)

vars_sel %in% vars_menu
missing_vars <- which(!vars_sel %in% vars_menu)
print('WARNING: the following attributes, as specified, were not found in the attribute menu:')
print(vars_sel[missing_vars])
