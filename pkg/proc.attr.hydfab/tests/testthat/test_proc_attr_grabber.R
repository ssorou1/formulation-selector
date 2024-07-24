#' @title Unit test attribute grabber processor
#' @description Unit testing for catchment attribute grabbing via the hydrofabric
#' @author Guy Litt \email{guy.litt@noaa.gov}


# Changelog / Contributions
#   2024-07-24 Originally created, GL


# unloadNamespace("proc.attr.hydfab")
library(proc.attr.hydfab)
library(testthat)
library(dplyr)
library(arrow)
library(hydrofabric)

# TODO establish a basic config file to read in for this functionality
comid <- "18094981"#"02479560"#14138870# A small basin
s3_base <- "s3://lynker-spatial/tabular-resources"
s3_bucket <- 'lynker-spatial'
s3_path_hydatl <- glue::glue('{s3_base}/hydroATLAS/hydroatlas_vars.parquet')

# Testing variables
ha_vars <- c('pet_mm_s01', 'cly_pc_sav', 'cly_pc_uav') # hydroatlas variables
usgs_vars <- c('TOT_TWI','TOT_PRSNOW','TOT_POPDENS90','TOT_EWT','TOT_RECHG')

# Define data directories to a package-specific data path
dir_base <- system.file("extdata")
temp_dir <- tempdir()
dir_hydfab <- file.path(temp_dir,'hfab')


Retr_Params <- list(paths = list(dir_hydfab=dir_hydfab,
                                 s3_path_hydatl = s3_path_hydatl),
                    vars = list(usgs_vars = usgs_vars,
                                ha_vars = ha_vars)
)
# ---------------------------------------------------------------------------- #
#                              UNIT TESTING
# ---------------------------------------------------------------------------- #
testthat::test_that("proc_attr_std_hfsub_name standardized name generator", {
  testthat::expect_equal('hydrofab_testit_111.parquet',
               proc.attr.hydfab::proc_attr_std_hfsub_name(111,"testit",'parquet'))

})

# Read in data of expected format

testthat::test_that("proc_attr_wrap", {
  exp_dat <- readRDS(system.file("extdata", paste0("attrs_",comid,".Rds"), package="proc.attr.hydfab"))
  dat_all <- proc.attr.hydfab::proc_attr_wrap(comid,Retr_Params,
                                              lyrs='network',
                                              overwrite=TRUE )
  # saveRDS(dat_all,paste0("~/git/fsds/pkg/proc.attr.hydfab/inst/extdata/attrs_",comid,".Rds"))
  testthat::expect_identical(exp_dat,dat_all)
})

testthat::test_that("proc_attr_hydatl", {
  exp_dat <- readRDS(system.file("extdata", paste0("attrs_",comid,".Rds"), package="proc.attr.hydfab"))
  ha <- proc.attr.hydfab::proc_attr_hydatl(comid,s3_path_hydatl,ha_vars)
  testthat::expect_identical(ha,
                             exp_dat[,c('COMID',Retr_Params$vars$ha_vars)] %>%
                                dplyr::rename(hf_id="COMID")
                              )

})

testthat::test_that("proc_attr_usgs_nhd", {
  exp_dat <- readRDS(system.file("extdata", paste0("attrs_",comid,".Rds"), package="proc.attr.hydfab"))
  order_cols <- c('COMID',Retr_Params$vars$usgs_vars)
  usgs_meta <- proc.attr.hydfab::proc_attr_usgs_nhd(comid,usgs_vars) %>%
    data.table::setcolorder(order_cols)
  testthat::expect_equal(usgs_meta,
                             exp_dat[,c('COMID',Retr_Params$vars$usgs_vars)] %>%
                                data.table::as.data.table(),
  )

})

# TODO figure this one out
# testthat::test_that("proc_attr_hf small, all divides",
#  small_hf <- proc.attr.hydfab::proc_attr_hf(comid="14138870", dir_hydfab,
#                                             custom_name="{lyrs}_",ext = 'gpkg',
#                                             lyrs=c('divides','network')[2],
#                                             hf_cat_sel=TRUE, overwrite=TRUE)
#   testthat::expect_length(small_hf,2)
#   testthat::expect_equal(dim(small_hf)[1],750)
# })

# Delete all files created inside the hydfab temp dir
unlink(temp_dir)
