library(aws.s3)
library(readxl)


# Set AWS environment -----------------------------------------------------

Sys.setenv(
  ACCOUNT_ID = "",
  AWS_ACCESS_KEY_ID = "",
  AWS_SECRET_ACCESS_KEY = "",
  AWS_DEFAULT_REGION = "us-east-2"
)


# Functions to read the csv files accounting for parsing errors -----------

read_excel_allsheets <- function(filename, tibble = FALSE) {
  sheets <- readxl::excel_sheets(filename)
  x <- lapply(sheets, function(X) readxl::read_excel(filename, sheet = X))
  if(!tibble) x <- lapply(x, as.data.frame)
  names(x) <- sheets
  x
}

read_csv_clean1 <- function(filename){
  
  x = read_csv(filename, n_max = 1, col_names = FALSE, col_types = cols(.default = "c"))
  x
  
}

read_csv_clean2 <- function(filename){
  
  x = read_csv(filename, skip = 1975, col_names = FALSE, col_types = cols(.default = "c"))
  x
  
}

read_csv_clean3 <- function(filename){
  
  x = read_csv(filename, col_types = cols(.default = "c"))
  x
  
}



# Read csv files ----------------------------------------------------------

df_lapsers <- s3read_using(FUN = read_csv_clean3, bucket = "predicthealth-aetna", object = "merged_data_7_22.csv")

df_nonlapsers <- s3read_using(FUN = read_csv_clean3, bucket = "predicthealth-aetna", object = "2107_PRHE1_FILE01_V2.csv") %>% 
  filter(!is.na(FNAME))

df_aetna<- s3read_using(FUN = read_excel_allsheets, bucket = "predicthealth-aetna", object = "Tracker_9927.xlsx")

# Create seperate data frame for each sheet then merge into one ------------


H4835 <- as.data.frame(df_aetna[1]) %>% 
  mutate(Contract = "H4835")
names(H4835) <- sub(".*\\.", "", names(H4835))

H5521 <- as.data.frame(df_aetna[2]) %>% 
  mutate(Contract = "H5521")
names(H5521) <- sub(".*\\.", "", names(H5521))

H3312 <- as.data.frame(df_aetna[3]) %>% 
  mutate(Contract = "H3312")
names(H3312) <- sub(".*\\.", "", names(H3312))

H1608 <- as.data.frame(df_aetna[4]) %>% 
  mutate(Contract = "H1608")
names(H1608) <- sub(".*\\.", "", names(H1608))

df_aetnajoined <- H4835 %>% 
  full_join(H5521) %>% 
  full_join(H3312) %>% 
  full_join(H1608)

rm(H4835, H5521, H3312, H1608, df_aetna, read_csv_clean1, read_csv_clean2, read_csv_clean3, read_excel_allsheets)

df_nonlapsers <- df_nonlapsers %>% 
  select(-FILLER)


# Create merge ID ---------------------------------------------------------

df_nonlapsers <- df_nonlapsers %>% 
  mutate(mergeID = paste(tolower(Client_FIRST_NM), tolower(Client_LAST_NM), ZIP, sep = "-")) %>% 
  distinct(mergeID, .keep_all = TRUE)

df_aetnajoined <- df_aetnajoined %>% 
  mutate(mergeID = paste(tolower(FIRST_NM), tolower(LAST_NM), ZIP_CD, sep = "-")) %>% 
  distinct(mergeID, .keep_all = TRUE)


# Merge new aetna data with vendor data -----------------------------------

df_nonlapsers_merged <- df_nonlapsers %>% 
  inner_join(df_aetnajoined, by = "mergeID")
 
  
# Check what columns are not in new data set ------------------------------

lapsers <- names(df_lapsers)
non_lapsers <- names(df_nonlapsers_merged)

setdiff(lapsers, non_lapsers)

rm(lapsers, non_lapsers)


# Merge new data with old data --------------------------------------------

df <- df_lapsers %>% 
  full_join(df_nonlapsers_merged) %>% 
  select(-mergeID)


# Manually match unmatched data -------------------------------------------

df_notmatched_vendor <- df_nonlapsers %>% 
  filter(!(df_nonlapsers$mergeID %in% df_nonlapsers_merged$mergeID)) %>% 
  filter(!is.na(FNAME))

df_notmatched_aetna <- df_aetnajoined %>% 
  filter(!(df_aetnajoined$mergeID %in% df_nonlapsers_merged$mergeID)) %>% 
  drop_na(mergeID)

df_unmatched_fuzzy <- df_notmatched_vendor %>% 
  stringdist_inner_join(df_notmatched_aetna, by = "mergeID", max_dist = 3) %>% 
  select(mergeID.x, mergeID.y)

View(df_unmatched_fuzzy)

df_unmatched_fuzzy <- df_notmatched_vendor %>% 
  stringdist_inner_join(df_notmatched_aetna, by = "mergeID", max_dist = 3) %>% 
  select(-mergeID.x, -mergeID.y)

df <- df %>% 
  full_join(df_unmatched_fuzzy) %>% 
  mutate(Lapsed = ifelse(is.na(Voluntary.Disenrollment), 0, 1))

varnames <- as.data.frame(names(df))

write_csv(varnames, "varnames_merged.csv")

# Export to s3 bucket -----------------------------------------------------

s3write_using(df, FUN = write.csv,
             bucket = "predicthealth-aetna",
             object = "merged_data_nonlapsers_8_6.csv")


