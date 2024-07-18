#===============================================================================
## Script for assigning NHD ComID's to study sites
## Code author: L. Bolotin
#===============================================================================

## Call packages
# x <- c("sf", "rgdal", "raster", "tidyverse", "nhdplusTools", "beepr", "dataRetrieval")
# lapply(x, require, character.only = TRUE)
# rm(x)
library(tidyverse)
library(nhdplusTools)
library(dataRetrieval)
library(beepr)
library(sf)

## Set working directory to a file with the CAMELS IDs
setwd('/Users/laurenbolotin/Lauren/FSDS/results/')

dat <- read.table('camels_hydro.txt', sep = ";", header = TRUE)

#--------------------------------------------
## Bring in location data
#--------------------------------------------
sites <- unique(zeroPad(dat$gauge_id, 8))

## Format
sites <- paste0("USGS-", sites)

## Prepare a data frame where we will put the ComID's
sites <- factor(sites)
sites <- levels(sites) |>
  as.data.frame()

sites$COMID <- "" # leave blank for now, this is what we are going to populate
colnames(sites)[1] <- "GAGE_ID"

## It seems (after trial) that using a USGS ID to identify a COMID is preferable to using coordinates, so do that whenever possible
## As opposed to using the lat/long for the Rio Grande sites, use the USGS ID's for the nearest streamgauges
# Re-name the sites to these gauges and then change the names back after assigning the ComID

#--------------------------------------------
## Assign NHD ComID using USGS SiteID
#--------------------------------------------

## Create function
findCOMID_USGS_ID <- function(x){ # x = USGS SiteID
  nldi_nwis <- list(featureSource = "nwissite", featureID = x)
  (sites$COMID[which(sites$GAGE_ID == x )] <<- discover_nhdplus_id(nldi_feature = nldi_nwis))
}
lapply(sites$GAGE_ID, findCOMID_USGS_ID)
beep() # Notifies us when the function above is done running with a sound


#--------------------------------------------
## Assign NHD ComID using lat/long
#--------------------------------------------
siteNumbers <- substr(sites$GAGE_ID, 6, 14)
site_metadata <- readNWISsite(siteNumbers) |> select(c(site_no, dec_lat_va, dec_long_va))

## Create function
findCOMID_USGS_coords <- function(x){ # x = USGS GAGE_ID
  tryCatch(point <- st_sfc(st_point(c((site_metadata$dec_long_va[which(site_metadata$site_no == substr(x, 6, 14))]), (site_metadata$dec_lat_va[which(site_metadata$site_no == substr(x, 6, 14))]))), crs = 4269), error = function(e) NULL)
  tryCatch(sites$COMID[which(sites$GAGE_ID == x)] <<- discover_nhdplus_id(point), error = function(e) NULL)
}
lapply(sites$GAGE_ID, findCOMID_USGS_coords)
beep()

#----------------------------------------------
## Compare ComID's assigned by the two methods
#----------------------------------------------

