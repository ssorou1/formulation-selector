#' @title A test fixture for unit testing test_proc_attr_grabber.R
#' @description Ensures that setup and teardown acode runs consistently
library(withr)

local_temp_dir <- function(env = parent.frame()) {
  temp_dir <- tempdir()
  withr::defer(unlink(temp_dir), env)
  temp_dir
}
local_temp_dir2 <- function(env = parent.frame()) {
  temp_dir <- tempdir()
  withr::defer(unlink(temp_dir), env)
  temp_dir
}
