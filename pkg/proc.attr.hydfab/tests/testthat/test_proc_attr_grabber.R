#' @title Unit test attribute grabber processor
#' @description Unit testing for catchment attribute grabbing via the hydrofabric
#' @author Guy Litt \email{guy.litt@noaa.gov}


# Changelog / Contributions
#   2024-07-24 Originally created, GL

library(testthat)
library(proc.attr.hydfab)

comid <- 14138870# A small basin

testthat::test_that("proc_attr_std_hfsub_name standardized name generator", {
  testthat::expect_equal('hydrofab_testit_111.parquet',
               proc.attr.hydfab::proc_attr_std_hfsub_name(111,"testit",'parquet'))

})

testthat::test_that("proc_attr_hydatl", {
  # TODO Set temp dir

  proc_attr_hydatl(comid, s3_path)




})
