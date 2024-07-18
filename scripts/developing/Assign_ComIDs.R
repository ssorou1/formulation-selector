#===============================================================================
## Script for assigning NHD ComID's to study sites
## Code author: L. Bolotin
#===============================================================================

# Call packages
x <- c("sf", "tidyverse", "nhdplusTools", "beepr", "dataRetrieval")
lapply(x, require, character.only = TRUE)
rm(x)

## Set working directory to a file with the CAMELS IDs
setwd('/Users/laurenbolotin/Lauren/FSDS/results/')

#--------------------------------------------
## Bring in site data
#--------------------------------------------
dat <- read.table('camels_hydro.txt', sep = ";", header = TRUE)

## Format
sites <- unique(zeroPad(dat$gauge_id, 8))
rm(dat)

sites <- paste0("USGS-", sites)

## Prepare a data frame where we will put the ComID's
sites <- factor(sites)
sites <- levels(sites) |>
  as.data.frame()

sites$COMID <- "" # leave blank for now, this is what we are going to populate
colnames(sites)[1] <- "GAGE_ID"

## It seems (after trial) that using a USGS ID to identify a COMID is preferable 
## to using coordinates, so do that whenever possible

#--------------------------------------------
## Assign NHD ComID using USGS SiteID
#--------------------------------------------

## Create function
findComidUsgsID <- function(x){ # x = USGS SiteID
  tryCatch(nldi_nwis <- list(featureSource = "nwissite", featureID = x), error = function(e) NULL)
  tryCatch(sites$COMID[which(sites$GAGE_ID == x )] <<- discover_nhdplus_id(nldi_feature = nldi_nwis), error = function(e) NULL)
}
lapply(sites$GAGE_ID, findComidUsgsID)
beep() # Notifies us when the function above is done running with a sound
usgs_comids <- sites

#--------------------------------------------
## Assign NHD ComID using lat/long
#--------------------------------------------
site_numbers <- substr(sites$GAGE_ID, 6, 14)
site_metadata <- readNWISsite(site_numbers) |> select(c(site_no, dec_lat_va, dec_long_va))

## Create function
findComidCoords <- function(x){ # x = USGS GAGE_ID
  tryCatch(point <- st_sfc(st_point(c((site_metadata$dec_long_va[which(site_metadata$site_no == substr(x, 6, 14))]), (site_metadata$dec_lat_va[which(site_metadata$site_no == substr(x, 6, 14))]))), crs = 4269), error = function(e) NULL)
  tryCatch(sites$COMID[which(sites$GAGE_ID == x)] <<- discover_nhdplus_id(point), error = function(e) NULL)
}
lapply(sites$GAGE_ID, findComidCoords)
beep()
coords_comids <- sites
rm(sites, site_numbers, site_metadata)

#----------------------------------------------
## Compare ComID's assigned by the two methods
#----------------------------------------------
comp_comids <- merge(coords_comids, usgs_comids, by = 'GAGE_ID')
n_matches = sum(comp_comids$COMID.x == comp_comids$COMID.y)
n_mismatches <-  nrow(comp_comids) - n_matches

# TODO: Add some sort of functionality that checks ComIDs assigned by lat/long
# to make sure that the ComID it chooses is actually in the same drainage area
