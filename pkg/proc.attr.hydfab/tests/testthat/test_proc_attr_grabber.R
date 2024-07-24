#' @title Unit test attribute grabber processor
#' @description Unit testing for catchment attribute grabbing via the hydrofabric
#' @author Guy Litt \email{guy.litt@noaa.gov}


# Changelog / Contributions
#   2024-07-24 Originally created, GL

library(testthat)
#unloadNamespace("proc.attr.hydfab")
library(proc.attr.hydfab)
library(dplyr)

# TODO establish a basic config file to read in for this functionality
comid <- "02479560"#14138870# A small basin
s3_base <- "s3://lynker-spatial/tabular-resources"
s3_bucket <- 'lynker-spatial'
s3_path_hydatl <- glue::glue('{s3_base}/hydroATLAS/hydroatlas_vars.parquet')

# Testing variables
ha_vars <- c('pet_mm_s01', 'cly_pc_sav', 'cly_pc_uav') # hydroatlas variables
usgs_vars <- c('TOT_TWI','TOT_PRSNOW','TOT_POPDENS90','TOT_EWT','TOT_RECHG')

# TODO change these directories to a package-specific  data path
home_dir <- Sys.getenv("HOME")
dir_base <- file.path(home_dir,'noaa/regionalization/data')
dir_std_base <- file.path(dir_base,"input/user_data_std")
dir_hydfab <- file.path(dir_base,'input/hydrofabric')
dir_hydfab <- file.path(home_dir,'noaa','hydrofabric')

Retr_Params <- list(paths = list(dir_hydfab=dir_hydfab,
                                 s3_path_hydatl = s3_path_hydatl),
                    vars = list(usgs_vars = usgs_vars,
                                ha_vars = ha_vars)
)



testthat::test_that("proc_attr_std_hfsub_name standardized name generator", {
  testthat::expect_equal('hydrofab_testit_111.parquet',
               proc.attr.hydfab::proc_attr_std_hfsub_name(111,"testit",'parquet'))

})

testhtat::test_that("proc_attr_wrap", {
  dat_all <- proc.attr.hydfab::proc_attr_wrap(comid,lyrs='network',overwrite=FALSE )



})

testthat::test_that("proc_attr_hydatl", {
  # TODO Set temp dir

  ha <- proc.attr.hydfab::proc_attr_hydatl(comid, s3_path_hydatl,ha_vars)




})
