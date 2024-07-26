#' @title Unit test attribute grabber processor
#' @description Unit testing for catchment attribute grabbing via the hydrofabric
#' @author Guy Litt \email{guy.litt@noaa.gov}


# Changelog / Contributions
#   2024-07-24 Originally created, GL


# unloadNamespace("proc.attr.hydfab")
suppressPackageStartupMessages(library(proc.attr.hydfab,quietly=TRUE))
suppressPackageStartupMessages(library(testthat,quietly=TRUE))
suppressPackageStartupMessages(library(dplyr,quietly=TRUE))
suppressPackageStartupMessages(library(arrow,quietly=TRUE))
suppressPackageStartupMessages(library(hydrofabric,quietly=TRUE))
suppressPackageStartupMessages(library(data.table,quietly=TRUE))
# TODO establish a basic config file to read in for this functionality
comid <- "18094981"#"02479560"#14138870# A small basin
s3_base <- "s3://lynker-spatial/tabular-resources"
s3_bucket <- 'lynker-spatial'
s3_path_hydatl <- glue::glue('{s3_base}/hydroATLAS/hydroatlas_vars.parquet')

# Testing variables
ha_vars <- c('pet_mm_s01', 'cly_pc_sav', 'cly_pc_uav') # hydroatlas variables
usgs_vars <- c('TOT_TWI','TOT_PRSNOW','TOT_POPDENS90','TOT_EWT','TOT_RECHG')

# Define data directories to a package-specific data path
dir_base <- system.file("extdata",package="proc.attr.hydfab")
temp_dir <- tempdir()
dir_hydfab <- file.path(temp_dir,'hfab')
dir_db_attrs <- file.path(temp_dir,'attrs') # used for temporary attr retrieval
dir_db_attrs_pkg <- system.file("extdata","attributes_pah",package="proc.attr.hydfab")# permanent pacakage location

Retr_Params <- list(paths = list(dir_hydfab=dir_hydfab,
                                 dir_db_attrs=dir_db_attrs,
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


testthat::test_that('retrieve_attr_exst', {
  comids <- c("1520007","1623207","1638559","1722317")
  vars <- Retr_Params$vars %>% unlist() %>% unname()

  # Run tests based on expected dims
  dat_attr_all <- proc.attr.hydfab::retrieve_attr_exst(comids,vars,dir_db_attrs_pkg)
  testthat::expect_equal(length(unique(dat_attr_all$COMID)),
                         length(comids))
  testthat::expect_equal(length(unique(dat_attr_all$attribute)),length(vars))

  testthat::expect_error(proc.attr.hydfab::retrieve_attr_exst(comids,
                                                              vars,
                                                              dir_db_attrs='a'))
  # Testing for No parquet files present
  capt_no_parquet <- testthat::capture_condition(proc.attr.hydfab::retrieve_attr_exst(comids,
                                                                vars,
                                                                dir_db_attrs=dirname(dir_db_attrs_pkg)))
  testthat::expect_true(grepl("parquet",capt_no_parquet$message))
  nada_var <- testthat::capture_warning(proc.attr.hydfab::retrieve_attr_exst(comids,vars=c("TOT_TWI","naDa"),
                                              dir_db_attrs_pkg))
  testthat::expect_true(grepl("naDa",nada_var$message))
  nada_comid <- testthat::capture_condition(proc.attr.hydfab::retrieve_attr_exst(comids=c("1520007","1623207","nada"),vars,
                                              dir_db_attrs_pkg))
  testthat::expect_true(base::grepl("nada",nada_comid$message))

  testthat::expect_error(proc.attr.hydfab::retrieve_attr_exst(comids,vars=c(3134,3135),
                                            dir_db_attrs_pkg))
  testthat::expect_warning(proc.attr.hydfab::retrieve_attr_exst(comids=c(3134,3135),vars,
                                            dir_db_attrs_pkg))
})

# Read in data of expected format

testthat::test_that("proc_attr_wrap", {
  exp_dat <- readRDS(system.file("extdata", paste0("attrs_",comid,".Rds"), package="proc.attr.hydfab"))
  dat_all <- proc.attr.hydfab::proc_attr_wrap(comid,Retr_Params,
                                              lyrs='network',
                                              overwrite=TRUE )
  # How the exp_dat was originally created for unit testing
  # saveRDS(dat_all,paste0("~/git/fsds/pkg/proc.attr.hydfab/inst/extdata/attrs_",comid,".Rds"))
  testthat::expect_true(dir.exists(dir_db_attrs))
  # testthat::expect_true(all(exp_dat==dat_all))
  # testthat::expect_equal(exp_dat,dat_all)

  # Test when data exist in tempdir and new data do not exist
  Retr_Params_only_new <- Retr_Params
  Retr_Params_only_new$vars$usgs_vars <- c('TOT_PET')
  dat_add_pet <- suppressWarnings(proc.attr.hydfab::proc_attr_wrap(comid,Retr_Params_only_new,
                                   lyrs='network',
                                   overwrite=FALSE ))
  testthat::expect_true(any('TOT_PET' %in% dat_add_pet$attribute))
  testthat::expect_true(any(grepl("TOT_PRSNOW", dat_add_pet$attribute)))

  # Test when some data exist in tempdir and new data needed
  Retr_Params_add <- Retr_Params
  Retr_Params_add$vars$usgs_vars <- c("TOT_TWI","TOT_PRSNOW","TOT_POPDENS90","TOT_EWT","TOT_RECHG","TOT_BFI")
  dat_all_bfi <- suppressWarnings(proc.attr.hydfab::proc_attr_wrap(comid,Retr_Params_add,
                                              lyrs='network',
                                              overwrite=FALSE ))
  testthat::expect_true(any('TOT_BFI' %in% dat_all_bfi$attribute))
  testthat::expect_true(any(grepl("TOT_PRSNOW", dat_all_bfi$attribute)))
})

testthat::test_that("proc_attr_hydatl", {
  exp_dat <- readRDS(system.file("extdata", paste0("ha_",comid,".Rds"), package="proc.attr.hydfab"))
  ha <- proc.attr.hydfab::proc_attr_hydatl(comid,s3_path_hydatl,ha_vars)
  # saveRDS(ha,paste0("~/git/fsds/pkg/proc.attr.hydfab/inst/extdata/ha_",comid,".Rds"))
  # Wide data expected
  testthat::expect_identical(ha,exp_dat)
})

testthat::test_that("proc_attr_usgs_nhd", {
  exp_dat <- readRDS(system.file("extdata", paste0("nhd_",comid,".Rds"), package="proc.attr.hydfab"))
  order_cols <- c('COMID',Retr_Params$vars$usgs_vars)
  usgs_meta <- proc.attr.hydfab::proc_attr_usgs_nhd(comid,usgs_vars) %>%
    data.table::setcolorder(order_cols)
  #saveRDS(usgs_meta,paste0("~/git/fsds/pkg/proc.attr.hydfab/inst/extdata/nhd_",comid,".Rds"))
  # Wide data expected
  # Check that the COMID col is returned (expected out of USGS data)
  testthat::expect_true(base::any(base::grepl("COMID",colnames(usgs_meta))))
  # Check for expected dataframe format
  testthat::expect_equal(usgs_meta,exp_dat)

})


testthat::test_that("proc_attr_hf not a comid",{
 testthat::expect_error(proc.attr.hydfab::proc_attr_hf(comid="13Notacomid14", dir_hydfab,
                                                       custom_name="{lyrs}_",ext = 'gpkg',
                                                       lyrs=c('divides','network')[2],
                                                       hf_cat_sel=TRUE, overwrite=FALSE))
})

testthat::test_that("proc_attr_exst_wrap", {

  ls_rslt <- proc.attr.hydfab::proc_attr_exst_wrap(comid,
                                                   path_attrs=dir_db_attrs,
                                                   vars_ls=Retr_Params$vars,
                                                   bucket_conn=NA)
  testthat::expect_true(all(names(ls_rslt) == c("dt_all","need_vars")))
  testthat::expect_type(ls_rslt,'list')
  testthat::expect_s3_class(ls_rslt$dt_all,'data.table')
  testthat::expect_true(nrow(ls_rslt$dt_all)>0)

  # Testing for a comid that doesn't exist
  new_dir <- base::tempdir()
  ls_no_comid <- proc.attr.hydfab::proc_attr_exst_wrap(comid='notexist134',
                                                      path_attrs=paste0(new_dir,'/newone/file.parquet'),
                                                      vars_ls=Retr_Params$vars,
                                                      bucket_conn=NA)
  testthat::expect_true(nrow(ls_no_comid$dt_all)==0)
  # Kinda-sorta running the test, but only useful if new_dir exists
  testthat::expect_equal(dir.exists(new_dir),
                         dir.exists(paste0(new_dir,'/newone')))
})
