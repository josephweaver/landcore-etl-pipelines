library(tidyverse)
library(sjlabelled)
library(binaryLogic)



#write functions for RCI calc
crop_number <- function(row){
  cn <- length(unique(row))
  return(cn)
}

crop_turnover.firstorder <- function(row) {
  turnover <- sum(as.binary(diff(row, 1), logic = TRUE))
  return(turnover)
}

crop_turnover.secondorder <- function(row) {
  turnover <- sum(as.binary(diff(row, 2), logic = TRUE))
  return(turnover)
}

#define function that recognizes the number of consecutive duplicates (excluding NAs) in a dataset, then sums them.
perennials <- function(row){
  duplicates <- rle(row)
  sum <- sum(duplicates$lengths - 1)
  return(sum)
}


tile.files <- list.files("~/Google Drive/Shared drives/Risk Model 2021 2 MVP Development/Data/Aggregated_By_Tile/CDL/all_years")

results.bound <- as.data.frame(matrix(nrow = 1, ncol = 7))
colnames(results.bound) <- c("number", "turnover", "RCIsq", "RCI", "year", "field_ID", "tile"  )

for(i in 1:length(tile.files)){

  tile <- read.table(file = paste("~/Google Drive/Shared drives/Risk Model 2021 2 MVP Development/Data/Aggregated_By_Tile/CDL/all_years/", tile.files[i], sep = ""),
                     header = T, sep = ",", dec = ".")
  
  tile.short <- tile %>% select(field_ID, cdl_class, year)
  
  tile.wide <- reshape(tile.short, idvar = "field_ID", timevar = "year", direction = "wide")
  
  tile.list <- list()

  for (j in 7:ncol(tile.wide)) {
    focal_year <- substring(paste(colnames(tile.wide)[j]), 11, 14)
    tile.list[[j-6]] <-  tile.wide[,c(1,(j-5):j)]
    names(tile.list)[j-6] <- paste("cdl", "end", focal_year, sep = "_")
  }

  results.all.years <- as.data.frame(matrix(nrow = 1, ncol = 7))
  colnames(results.all.years) <- c("number", "turnover", "RCIsq", "RCI", "year", "field_ID", "tile"  )

  for (k in 1:length(tile.list)){
    tile.df1 <- tile.list[[k]]
    tile.df <- set_na(tile.df1[,2:7], na=c(0, 81, 82, 83, 86, 88)) #convert all non-ag to NA
    tile.df$field_ID <- tile.df1$field_ID
    tile.df <- tile.df[complete.cases(tile.df), ] #filter out any rows with NA
    if(nrow(tile.df)!=0) {
      rownames(tile.df) <- tile.df$field_ID
      tile.df <- tile.df %>% select(!field_ID)
      transition.matrix <- matrix(nrow = nrow(tile.df), ncol = 3)
      crop_results.df <- as.data.frame(transition.matrix)
      rownames(crop_results.df) <- rownames(tile.df)
      colnames(crop_results.df) <- c("number", "turnover.first", "turnover.second")
      crop_results.df$number <- apply(tile.df, 1, crop_number)
      crop_results.df$turnover.first <- apply(tile.df, 1, crop_turnover.firstorder)
      crop_results.df$turnover.second <- apply(tile.df, 1, crop_turnover.secondorder)
      
      #perennial correction
      
      all_crops <- unique(unlist(tile.df))
      non_perennial <- all_crops[!all_crops %in% c(36, 37, 61, 176, 224)] #alfalfa, hay, idle/fallow, grass/pasture
      perennials.df <- set_na(tile.df, na=non_perennial) #convert all non-perennial to NA
      crop_results.df$turnover_correction <- apply(perennials.df, 1, perennials) #run the function
      crop_results.df$turnover1_corrected <- crop_results.df$turnover.first + crop_results.df$turnover_correction #add correction
      crop_results.df$turnover_correction <- NULL
      crop_results.df$turnover.first <- NULL
      crop_results.df$turnover <- (crop_results.df$turnover.second + crop_results.df$turnover1_corrected)/2 #create turnover: mean of turnovers 1 & 2
      crop_results.df$turnover.second <- NULL
      crop_results.df$turnover1_corrected <- NULL
      crop_results.df$RCIsq <- crop_results.df$turnover * crop_results.df$number
      crop_results.df$RCI <- round(sqrt(crop_results.df$RCIsq), 2)
      crop_results.df$year <- substring(paste(colnames(tile.df)[6]), 11, 14)
      crop_results.df$field_ID <- rownames(crop_results.df)
      crop_results.df$tile <- tile$tile_coord[1]
      rownames(crop_results.df) <- NULL
      results.all.years <- rbind(results.all.years, crop_results.df)
      }
    }
  results.bound <- rbind(results.bound, results.all.years)
  print(tile.files[i])
}

save(results.bound, file = "~/Google Drive/Shared drives/Risk Model 2021 2 MVP Development/Data/All_Tile_Aggregated/RCI_cdl_aggregated_by_field.Rdata")

#assign(paste("cdl", "end", focal_year, sep = "_"), tile.wide[,c(1,(i-5):i)]) #useful for later!