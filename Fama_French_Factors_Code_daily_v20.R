# REPLICATE FAMA-FRENCH-CARHART 4-FACTOR MODEL DAILY
# Code inspired by Wayne Chang (https://sites.google.com/site/waynelinchang/home)
# Based on methodology described on Ken French website and relevant Fama-French papers
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Log ####
# v6:
# - remove unneeded function in portfolio construction
# - remove monthly construction
# - remove data.comp.fundq
# v7:
# - add weekly computation
#  v8:
# - changed NA handling in factor construction using coalesce() and mean with na.rm = T; 
# - we assume that no sentiment means that the prrevious sentiment level is kept 
# v9:
# - added library for dplyr logs on operations
# - for investigation, we change above assumption and treat NAs as 0
# - correction in regression model where only a subset of data was used due to merger issue in Yearweek data
# v10:
# - introduced handling for infinites and NANs to be treated as NAs in sentiment
# - change computation of sentiment diff from relative to absolute due to very high outliers
# - changed merge to join were possible for better stats
# - constructed equidistant time series for TRMi
# - fixed error for regressions with settings 3 / 2
# v12:
# - split sentiment
# v13:
# -	change sentiment calculation to relative again
# v14:
# -	change sentiment calculation to absolute again
# -	commented #Fill_TS_NAs_TRMI(quo(Day))
# v15:
# - uncommented Fill_TS_NAs_TRMI(quo(Day))
# - Added regressions on higher moments of PMN
# - added assessment of coefficients of regressions
# v16:
# - remove NA handling from TRMI construction 
# v17:
# -	Removed all NA handling for portfolio and factor construction for sentiment
# v18:
# -	based on version 16
# - scale snt.glo.diff
# v19:
# -	based on version 16
# - reduce to 2006 tp 2017
# v20:
# - introduce neuralnet
# - changed to git versioning 
# SETUP ####
# remove all objects from console for a fresh start
rm(list = ls())
gc() 
library(tidyverse); library(zoo) 
library(data.table); library(lubridate)
library(psych)
library(ggplot2)
library(GRS.test) # for GRS statistic
library(broom) # for complex returned statistical objects from regressions
library(ramify) # for matrix resizing
library(purrr) # for chunk wise operations
library(taRifx) # to merge two lists
library(RPostgres)
library(tsibble) # for yearweek
library("tidylog", warn.conflicts = FALSE) # to get stats on operations
library(neuralnet) #for neural networks
# library(RevoScaleR)
library(usethis) #git
library(testthat) # unit tests
library(qdapTools) # for list to df transformations


setwd("/scratch/unisg/ck/new_weekly_lagged/")

# Alternative filepath
filepath <- "/scratch/unisg/ck/new_weekly_lagged/"

### WRDS CONNECTION ###
wrds <- dbConnect(Postgres(),
                  host = 'wrds-pgdata.wharton.upenn.edu',
                  port = 9737,
                  user = 'ckoeppel',
                  password = 'BayInv01!',
                  dbname = 'wrds',
                  sslmode = 'require')

# Load file ####
load(paste(filepath, "FF5_Sentiment_daily.RData", sep = ""))

# Save File ####
save.image(paste(filepath, "FF5_Sentiment_daily.RData", sep = ""))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# LOAD COMPUSTAT FROM WRDS ####
res <- dbSendQuery(wrds,"select GVKEY, CUSIP, DATADATE, FYR, FYEAR, SICH, NAICSH, 
                   AT, LT, SEQ, CEQ, PSTKL, PSTKRV, PSTK, TXDITC, TXDB, ITCB,
                   REVT, COGS, XINT, XSGA, IB, TXDI, DVC, ACT, CHE, LCT,
                   DLC, TXP, DP, PPEGT, INVT, MIB 
                   from COMP.FUNDA
                   where INDFMT = 'INDL' and DATAFMT = 'STD' and CONSOL = 'C' and POPSRC = 'D'") # STD is unrestatd data

data.comp.funda <- dbFetch(res, n = -1) # n = -1 denotes no max but retrieve all record
save(data.comp.funda, file = paste(filepath, "data.comp.funda.RData", sep = ""))

# Retrieve Merged Compustat/CRSP link table ####
res <- dbSendQuery(wrds,"select GVKEY, LPERMNO, LINKDT, LINKENDDT, LINKTYPE, LINKPRIM
                   from crsp.ccmxpf_lnkhist")

data.ccmlink <- dbFetch(res, n = -1) 
save(data.ccmlink, file = paste(filepath, "data.ccmlink.RData", sep = ""))

# Merge the linked Permno onto Compustat dataset ####
# compared to SAS code based on WRDS FF Research macro, I don't include all Linktypes but add J Linkprim
# including J linkprim is key bc/ allows me to get the post-2010 Berkshire history
# omitting non-primary linktypes led to 1% fewer obs (2,000) but cleaner data (datadate< = "2013-12-31" for comparability)
data.ccm <-  data.ccmlink %>%
  # use only primary links (from WRDS Merged Compustat/CRSP examples)
  filter(linktype %in% c("LU", "LC", "LS")) %>%
  filter(linkprim %in% c("P", "C", "J")) %>%
  merge(data.comp.funda, by = "gvkey") %>% # inner join, keep only if permno exists
  mutate(datadate = as.Date(datadate), 
         permno = as.factor(lpermno),
         linkdt = as.Date(linkdt),
         linkenddt = as.Date(linkenddt),
         linktype = factor(linktype, levels = c("LC", "LU", "LS")),
         linkprim = factor(linkprim, levels = c("P", "C", "J"))) %>%
  # remove compustat fiscal ends that do not fall within linked period; linkenddt = NA (from .E) means ongoing  
  filter(datadate >=  linkdt & (datadate <=  linkenddt | is.na(linkenddt))) %>%
  # prioritize linktype, linkprim based on order of preference/primary if duplicate
  arrange(datadate, permno, linktype, linkprim) %>%
  dplyr::distinct(datadate, permno, .keep_all = TRUE)

save(data.ccm, file = paste(filepath, "data.ccm.RData", sep = ""))
rm(data.comp.funda, data.ccmlink)

# # testing
# str(data.ccm)
# summary(data.ccm)
# View(filter(data.ccm, permno == 17778)) # berkshire
# View(filter(data.ccm, permno == 12060)) # ge

# COMPUSTAT CLEANING AND VAR CALC ####

# load(paste(filepath, "data.ccm.RData", sep = ""))
data.comp <- data.ccm %>%
  rename(PERMNO = permno) %>% 
  data.table %>% # ensure col names match crsp's
  arrange(datadate, PERMNO) %>%
  dplyr::distinct(datadate, PERMNO, .keep_all = TRUE) # hasn't been issue but just in case

save(data.comp, file = paste(filepath, "data.comp.RData", sep = ""))

# load(paste(filepath, "data.comp.RData", sep = ""))
data.comp.a <- data.comp %>%
  group_by(PERMNO) %>%
  mutate(BE = coalesce(seq, ceq + pstk, at - lt) + coalesce(txditc, txdb + itcb, 0) - 
           coalesce(pstkrv, pstkl, pstk, 0), # consistent w/ French website variable definitions
         OpProf = (revt - coalesce(cogs, 0) - coalesce(xint, 0) - coalesce(xsga,0)),
         OpProf = as.numeric(ifelse(is.na(cogs) & is.na(xint) & is.na(xsga), NA, OpProf)), # FF condition
         GrProf = (revt - cogs),
         Cflow = ib + coalesce(txdi, 0) + dp,  # operating; consistent w/ French website variable definitions
         Inv = (coalesce(ppegt - lag(ppegt), 0) + coalesce(invt - lag(invt), 0)) / lag(at),
         AstChg = (at - lag(at)) / lag(at) # note that lags use previously available (may be different from 1 yr)
  ) %>% 
  ungroup %>%
  arrange(datadate, PERMNO) %>%
  select(datadate, PERMNO, naicsh, at, revt, ib, dvc, mib, BE:AstChg) %>% #[CK] REMOVED comp.count
  mutate_if(is.numeric, list(~ ifelse(is.infinite(.), NA, .))) %>% # replace Inf w/ NA's
  mutate_if(is.numeric, list(~ round(., 5))) # round to 5 decimal places (for some reason, 0's not properly coded in some instances)

save(data.comp.a, file = paste(filepath, "data.comp.a.RData", sep = ""))
rm(data.ccm, data.comp)

# testing
# summary(data.comp.a)
# str(data.comp.a) # check data
# data.comp.a %>% filter(PERMNO == '17144') %>% View # general mills

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# LOAD DAILY CRSP FROM WRDS ####
# Downloads CRSP MSE, MSF, and MSEDELIST tables from WRDS
# merges, cleans, and for market cap calc, combines permco's with multiple permnos (eg berkshire)

# Load Daily stock file ####
compute.spread.fct <- function(df){
  df <- df %>%
    select(bidlo, askhi, prc) %>%
    rowwise %>%
    mutate(spread = case_when(
      prc == 0 & (bidlo - askhi) < 0 ~ bidlo,
      prc == 0 & (bidlo - askhi) > 0 ~ askhi,
      prc > 0 ~ bidlo - askhi,
      TRUE ~ 0
    ))
}

res <- dbSendQuery(wrds, "select DATE, PERMNO, PERMCO, HSICCD, HEXCD, BIDLO, ASKHI, RET, CFACPR, CFACSHR, SHROUT, PRC, RETX, VOL
                   from CRSP.DSF")

crsp.dsf <- dbFetch(res, n = -1) 

crsp.dsf <- crsp.dsf %>%
  mutate(spread = case_when(
    prc == 0 & (bidlo - askhi) < 0 ~ bidlo,
    prc == 0 & (bidlo - askhi) > 0 ~ askhi,
    prc > 0 ~ bidlo - askhi,
    TRUE ~ 0
  ))  %>%
  filter(!is.na(prc)) %>%
  mutate(Day = as.Date(date)) %>%
  select(-date)

save(crsp.dsf, file = paste(filepath, "crsp.dsf.RData", sep = ""))

res <- dbSendQuery(wrds, "select DATE, PERMNO, SHRCD, EXCHCD, NAICS
                   from CRSP.DSE")

crsp.dse <- dbFetch(res, n = -1)

crsp.dse <- crsp.dse %>%
  filter(!is.na(shrcd)) %>%
  mutate(Day = as.Date(date)) %>%
  select(-date)

save(crsp.dse, file = paste(filepath, "crsp.dse.RData", sep = ""))

res <- dbSendQuery(wrds, "select DLSTDT, PERMNO, dlret
                   from crsp.dsedelist") 

crsp.dsedelist <- dbFetch(res, n = -1)

crsp.dsedelist <- crsp.dsedelist %>%
  filter(!is.na(dlret)) %>%
  mutate(Day = as.Date(dlstdt)) %>%
  select(-dlstdt)

save(crsp.dsedelist, file = paste(filepath, "crsp.dsedelist.RData", sep = ""))

# Merge daily CRSP data ####
load(paste(filepath, "crsp.dsf.RData", sep = ""))
load(paste(filepath, "crsp.dse.RData", sep = ""))
load(paste(filepath, "crsp.dsedelist.RData", sep = ""))

data.crsp.d2 <- crsp.dsf %>%
  join(crsp.dse, by = c("Day", "permno"), type = "full", match = "all") %>%
  join(crsp.dsedelist, by = c("Day", "permno"), type = "full", match = "all") %>%
  rename(PERMNO = permno) %>% 
  mutate_at(vars(PERMNO, permco, shrcd, exchcd), list(~ as.factor)) %>%
  mutate(retadj = ifelse(!is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) 

save(data.crsp.d2, file = paste(filepath, "data.crsp.d2.RData", sep = ""))

data.crsp.d3 <- data.crsp.d2 %>% # create retadj by merging ret and dlret
  arrange(Day, PERMNO) %>%
  group_by(PERMNO) %>%    
  mutate_at(vars(shrcd, exchcd), list(~ na.locf(., na.rm = FALSE))) %>% # fill in NA's with latest available (must sort by Date and group by PERMNO)
  mutate(meq = shrout * abs(prc)) %>% # me for each permno
  group_by(Day, permco) %>%
  mutate(ME = sum(meq)) %>% # to calc market cap, merge permnos with same permnco
  arrange(Day, permco, desc(meq)) %>%
  group_by(Day, permco)  

save(data.crsp.d3, file = paste(filepath, "data.crsp.d3.RData", sep = ""))

data.crsp.d4 <- data.crsp.d3 %>% 
  dplyr::slice(1) 

save(data.crsp.d4, file = paste(filepath, "data.crsp.d4.RData", sep = ""))

data.crsp.d <- data.crsp.d4 %>% # keep only permno with largest meq
  ungroup %>%
  dplyr::distinct(Day, PERMNO, .keep_all = T) # hasn't been issue but just in case

# Unit Test
# data.crsp.d.validation <- summary(data.crsp.d)
# save(data.crsp.d.validation, file = "data.crsp.d.validation.RData")
load("data.crsp.d.validation.RData")
expect_equal(summary(data.crsp.d), data.crsp.d.validation)
rm(data.crsp.d.validation)

save(data.crsp.d, file = paste(filepath, "data.crsp.d.RData", sep = ""))
rm(crsp.dse, crsp.dsf, crsp.dsedelist, data.crsp.d2, data.crsp.d3, data.crsp.d4)

# Daily CRSP CLEANING ####
# filters EXCHCD (NYSE, NASDAQ, AMEX) and SHRCD (10,11)
load(paste(filepath, "data.crsp.d.RData", sep = ""))

Fill_TS_NAs_Day <- function(main) {
  # takes datat frame with Date and PERMNO as columns and fills in NAs where there are gaps
  #browser()
  core <- select(main, Day, PERMNO)
  # find first and last dates of each PERMNO
  date.bookends <- core %>%
    group_by(PERMNO) %>%
    summarize(first = first(Day), last = last(Day))
  
  # generate all dates for all PERMNOs then trim those outside of each PERMNO's first and last dates
  output <- core %>%
    mutate(temp = 1) %>% # create 3rd column so spread can be applied
    spread(., PERMNO, temp) %>%
    gather(., PERMNO, temp, -Day) %>%
    merge(date.bookends, by = "PERMNO", all.x = TRUE) %>%
    group_by(PERMNO) %>%
    filter(Day >= first & Day <= last) %>%
    select(Day, PERMNO) %>%
    merge(., main, by = c("Day", "PERMNO"), all.x = TRUE)
  
  return(output)
}

# SLOW CODE (25 mins)
data.crsp.cln.d1 <- data.crsp.d %>%
  select(Day, PERMNO, shrcd, exchcd, naics, cfacpr, cfacshr, shrout, prc, vol, retx, retadj, ME) %>%
  mutate(ME = ME/1000) %>%  # convert from thousands to millions (consistent with compustat values)
  filter((shrcd  ==  10 | shrcd  ==  11) & (exchcd  ==  1 | exchcd  ==  2 | exchcd  ==  3))

save(data.crsp.cln.d1, file = paste(filepath, "data.crsp.cln.d1.RData", sep = ""))

load(paste(filepath, "data.crsp.cln.d1.RData", sep = ""))

data.crsp.cln.d2 <- data.crsp.cln.d1 %>%
  filter(Day <= "1990-01-01")
  Fill_TS_NAs_Day 

data.crsp.cln.d3 <- data.crsp.cln.d1 %>%
    filter(Day > "1990-01-01")
  Fill_TS_NAs_Day 

data.crsp.cln.d <- data.crsp.cln.d2 %>%
  rbind(data.crsp.cln.d3) %>% # fill in gap dates within each PERMNO with NAs to uses lead/lag (lead to NAs for SHRCD and EXCHCD); fn in AnoDecomp_Support
  mutate(PERMNO = as.factor(PERMNO)) %>%
  group_by(PERMNO) %>%
  mutate(port.weight = as.numeric(ifelse(!is.na(lag(ME)), lag(ME), ME/(1 + retx))), # calc portweight as ME at beginning of period
         port.weight = ifelse(is.na(retadj) & is.na(prc), NA, port.weight)) %>% # remove portweights calc for date gaps
  ungroup %>% 
  rename(retadj.1mn = retadj) %>%
  arrange(Day, PERMNO) %>%
  dplyr::distinct(Day, PERMNO, .keep_all = T) # hasn't been issue but just in case

# Unit Test
# data.crsp.cln.d.validation <- summary(data.crsp.cln.d)
# save(data.crsp.cln.d.validation, file = "data.crsp.cln.d.validation.RData")
load("data.crsp.cln.d.validation.RData")
expect_equal(summary(data.crsp.cln.d), data.crsp.cln.d.validation)
rm(data.crsp.cln.d.validation)

save(data.crsp.cln.d, file = paste(filepath, "data.crsp.cln.d.RData", sep = ""))
rm(data.crsp.d, data.crsp.cln.d1, data.crsp.cln.d2, data.crsp.cln.d3)

# testing
# str(data.crsp.cln) # check data
# summary(data.crsp.cln)
# data.crsp.cln %>% filter(PERMNO == '17144') %>% View # general mills
# data.crsp.cln %>% filter(PERMNO == '10007') %>% View # gap NAs filled

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# LOAD Monthly CRSP FROM WRDS ####
# Downloads CRSP MSE, MSF, and MSEDELIST tables from WRDS
# merges, cleans, and for market cap calc, combines permco's with multiple permnos (eg berkshire)

# Load Monthly stock file ####
res <- dbSendQuery(wrds, "select DATE, PERMNO, PERMCO, HSICCD, HEXCD, BIDLO, ASKHI, SPREAD, RET, CFACPR, CFACSHR, SHROUT, PRC, RET, RETX, VOL
                   from CRSP.MSF")

crsp.msf <- dbFetch(res, n = -1) 

crsp.msf <- crsp.msf %>%
  filter(!is.na(prc)) %>%
  mutate(Yearmon = as.yearmon(as.Date(date))) %>%
  select(-date)

save(crsp.msf, file = paste(filepath, "crsp.msf.RData", sep = ""))

res <- dbSendQuery(wrds, "select DATE, PERMNO, SHRCD, EXCHCD, NAICS
                   from CRSP.MSE")

crsp.mse <- dbFetch(res, n = -1)

crsp.mse <- crsp.mse %>%
  filter(!is.na(shrcd)) %>%
  mutate(Yearmon = as.yearmon(as.Date(date))) %>%
  select(-date)

save(crsp.mse, file = paste(filepath, "crsp.mse.RData", sep = ""))

res <- dbSendQuery(wrds, "select DLSTDT, PERMNO, dlret
                   from crsp.msedelist") 

#changed to crsp.msdelist
crsp.msedelist <- dbFetch(res, n = -1)

crsp.msedelist <- crsp.msedelist %>%
  filter(!is.na(dlret)) %>%
  mutate(Yearmon = as.yearmon(as.Date(dlstdt))) %>%
  select(-dlstdt)

save(crsp.msedelist, file = paste(filepath, "crsp.msedelist.RData", sep = ""))
rm(wrds, res)

# Merge Monthly CRSP  data ####
# load(paste(filepath, "crsp.msf.RData", sep = ""))
# load(paste(filepath, "crsp.mse.RData", sep = ""))
# load(paste(filepath, "crsp.msedelist.RData", sep = ""))

data.crsp.m <- crsp.msf %>%
  merge(crsp.mse, by = c("Yearmon", "permno"), all = TRUE, allow.cartesian = TRUE) %>%
  merge(crsp.msedelist, by = c("Yearmon", "permno"), all = TRUE, allow.cartesian = TRUE) %>%
  rename(PERMNO = permno) %>% 
  mutate_at(vars(PERMNO, permco, shrcd, exchcd), list(~ as.factor)) %>%
  mutate(retadj = ifelse(!is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>% # create retadj by merging ret and dlret
  arrange(Yearmon, PERMNO) %>%
  group_by(PERMNO) %>%    
  mutate_at(vars(shrcd, exchcd), list(~ na.locf(., na.rm = FALSE)))  # fill in NA's with latest available (must sort by Date and group by PERMNO)

data.crsp.m <- data.crsp.m %>%
  mutate(meq = shrout * abs(prc)) %>% # me for each permno
  group_by(Yearmon, permco) %>%
  mutate(ME = sum(meq)) %>% # to calc market cap, merge permnos with same permnco
  arrange(Yearmon, permco, desc(meq)) %>%
  group_by(Yearmon, permco) %>%
  slice(1) %>% # keep only permno with largest meq
  ungroup

# Unit Test
# data.crsp.m.validation <- summary(data.crsp.m)
# save(data.crsp.m.validation, file = "data.crsp.m.validation.RData")
load("data.crsp.m.validation.RData")
expect_equal(summary(data.crsp.m), data.crsp.m.validation)
rm(data.crsp.m.validation)

save(data.crsp.m, file = paste(filepath, "data.crsp.m.RData", sep = ""))
rm(crsp.mse, crsp.msf, crsp.msedelist)

# Get Davis book equity data ####
# Merges CRSP and Compustat data fundamentals by PERMNO and DATE (annual-June-end portfolio formation)
# Also get Davis book equity data (Compustat match begins 1951 but Davis book data available starting 20s)
# Keep all CRSP info (drop Compustat if can't find CRSP)
# Match Compustat and Davis data based on FF methodology (to following year June when data is first known at month end)

data.Davis.bkeq <- read.csv(paste(filepath, "Davis Book Equity.csv", sep = ""))
data.Davis.bkeq[data.Davis.bkeq  ==  -999 | data.Davis.bkeq  ==  -99.99] <- NA
data.Davis.bkeq <- data.Davis.bkeq %>%
  mutate(PERMNO = factor(PERMNO)) %>%
  data.table %>%
  select(-FirstYr, -LastYr) %>%
  gather(Date, Davis.bkeq, -PERMNO, na.rm = TRUE) %>%
  mutate(Day = ymd(paste0(substr(Date, 2, 5),"-6-01")), Yearmon = as.yearmon(ymd(paste0(substr(Date, 2, 5),"-6-01")))) 

save(data.Davis.bkeq, file = paste(filepath, "data.Davis.bkeq.RData", sep = ""))

# set date June of SAME year when data would have been known (based on French website notes)

# Monthly CRSP CLEANING ####
# filters EXCHCD (NYSE, NASDAQ, AMEX) and SHRCD (10,11)

Fill_TS_NAs <- function(main) {
  # takes datat frame with Date and PERMNO as columns and fills in NAs where there are gaps
  
  core <- select(main, Yearmon, PERMNO)
  # find first and last dates of each PERMNO
  date.bookends <- core %>%
    group_by(PERMNO) %>%
    summarize(first = first(Yearmon), last = last(Yearmon))
  
  # generate all dates for all PERMNOs then trim those outside of each PERMNO's first and last dates
  output <- core %>%
    mutate(temp = 1) %>% # create 3rd column so spread can be applied
    spread(., PERMNO, temp) %>%
    gather(., PERMNO, temp, -Yearmon) %>%
    merge(date.bookends, by = "PERMNO", all.x = TRUE) %>%
    group_by(PERMNO) %>%
    filter(Yearmon >= first & Yearmon <= last) %>%
    select(Yearmon, PERMNO) %>%
    merge(., main, by = c("Yearmon", "PERMNO"), all.x = TRUE)
  
  return(output)
}

# SLOW CODE (25 mins)
# load(paste(filepath, "data.crsp.m.RData", sep = ""))
data.crsp.cln.m <- data.crsp.m %>%
  select(Yearmon, PERMNO, shrcd, exchcd, naics, cfacpr, cfacshr, shrout, prc, vol, retx, retadj, ME) %>%
  mutate(ME = ME/1000) %>%  # convert from thousands to millions (consistent with compustat values)
  filter((shrcd  ==  10 | shrcd  ==  11) & (exchcd  ==  1 | exchcd  ==  2 | exchcd  ==  3)) %>%
  Fill_TS_NAs %>% # fill in gap dates within each PERMNO with NAs to uses lead/lag (lead to NAs for SHRCD and EXCHCD); fn in AnoDecomp_Support
  mutate(PERMNO = as.factor(PERMNO)) %>%
  group_by(PERMNO) %>%
  mutate(port.weight = as.numeric(ifelse(!is.na(lag(ME)), lag(ME), ME/(1 + retx))), # calc portweight as ME at beginning of period
         port.weight = ifelse(is.na(retadj) & is.na(prc), NA, port.weight)) %>% # remove portweights calc for date gaps
  ungroup %>% 
  rename(retadj.1mn = retadj) %>%
  arrange(Yearmon, PERMNO) %>%
  dplyr::distinct(Yearmon, PERMNO, .keep_all = TRUE) # hasn't been issue but just in case

save(data.crsp.cln.m, file = paste(filepath, "data.crsp.cln.m.RData", sep = ""))
rm(data.crsp.m)

# Merge Compustat and Monthly CRSP ####
load(paste(filepath, "data.crsp.cln.m.RData", sep = ""))
load(paste(filepath, "data.comp.a.RData", sep = ""))
load(paste(filepath, "data.Davis.bkeq.RData", sep = ""))

na_locf_until = function(x, n) {
  # in time series data, fill in na's untill indicated n
  l <- cumsum(!is.na(x))
  c(NA, x[!is.na(x)])[replace(l, ave(l, l, FUN = seq_along) > (n + 1), 0) + 1]
}

data.both.m <- data.comp.a %>%  
  mutate(Yearmon = as.yearmon(datadate) + (18 - month(as.yearmon(datadate)))/12) %>% # map to next year June period when data is known (must occur in previous year)
  merge(data.crsp.cln.m, ., by = c("Yearmon", "PERMNO"), all.x = TRUE, allow.cartesian = TRUE) %>%  # keep all CRSP records (Compustat only goes back to 1950)
  merge(data.Davis.bkeq, by = c("Yearmon", "PERMNO"), all.x = TRUE, allow.cartesian = TRUE) %>%
  arrange(PERMNO, Yearmon, desc(datadate)) %>%
  dplyr::distinct(PERMNO, Yearmon, .keep_all = TRUE) %>% # drop older datadates (must sort by desc(datadate))
  group_by(PERMNO) %>%
  # fill in Compustat and Davis data NA's with latest available for subsequent year (must sort by Date and group by PERMNO)
  # filling max of 11 previous months means gaps may appear when fiscal year end changes (very strict)
  mutate_at(vars(datadate:Davis.bkeq), list(~ na_locf_until(., 11)))
  
save(data.both.m, file = paste(filepath, "data.both.m.RData", sep = "")) 
# company info has no Date gaps (filled with NA's)
# all data publicly available by end of Date period (Compustat first data is June-1950 matched to CRSP Jun-51))
# includes all CRSP (but only Compustat/Davis data that matches CRSP)
# CRSP first month price data Dec-25, return data Jan-26
# CRSP last month data Dec-17 (Compustat end data available but discarded bc/ must be mapped to CRSP end data)
# 3.507 MM obs (Old: 170226 3.463 MM obs)
rm(data.comp.a, data.crsp.cln.m, data.Davis.bkeq)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Construct FF Variables ####

get.mav <- function(bp,n=2){
  require(zoo)
  #if (is.na(bp[1])) bp[1] <- mean(bp,na.rm = TRUE)
  #bp <- na.locf(bp,na.rm = FALSE)
  if (length(bp) < n) return(bp)
  c(bp[1:(n - 1)],rollapply(bp,width = n,prod,align = "right", na.rm = TRUE))  
}

load(paste(filepath, "data.both.m.RData", sep = "")) 
data.both.FF.m.full <- data.both.m %>%
  group_by(PERMNO) %>%
  mutate(d.shares = (shrout*cfacshr)/(lag(shrout)*lag(cfacshr)) - 1, # change in monthly share count (adjusted for splits)
         ret.12t2 = lag(get.mav(1 + retadj.1mn, 11) - 1,1),# to calc momentum spread, 11 for 12 month spread
         BE = coalesce(BE, Davis.bkeq), # data available by end-of-Jun based on Compustat Date mapping 
         ME.Dec = as.numeric(ifelse(month(Yearmon) == 6 & lag(ME,6) > 0, lag(ME,6), NA)), # previous Dec ME 
         ME.Jun = as.numeric(ifelse(month(Yearmon) == 6, ME, NA)), 
         BM.FF = as.numeric(ifelse(month(Yearmon) == 6 & ME.Dec > 0, BE/ME.Dec, NA)), 
         OpIB = as.numeric(ifelse(month(Yearmon) == 6 & BE > 0, OpProf/(BE), NA)), 
         GrIA = as.numeric(ifelse(month(Yearmon) == 6 & at > 0, GrProf/at, NA)),
         CFP.FF = as.numeric(ifelse(month(Yearmon) == 6 & ME.Dec > 0, Cflow/ME.Dec, NA)),
         BM.m = BE/ME, # monthly updated version for spread calc
         CFP.m = Cflow/ME, # monthly updated version for spread calc
         lag.ME.Jun = lag(ME.Jun), # monthly data so only lag by 1 mn
         lag.BM.FF = lag(BM.FF),
         lag.ret.12t2 = lag(ret.12t2),
         lag.OpIB = lag(OpIB),
         lag.AstChg = lag(AstChg))

# Unit Test
# data.both.FF.m.full.validation <- summary(data.both.FF.m.full)
# save(data.both.FF.m.full.validation, file = "data.both.FF.m.full.validation.RData")
load("data.both.FF.m.full.validation.RData")
expect_equal(summary(data.both.FF.m.full), data.both.FF.m.full.validation)
rm(data.both.FF.m.full.validation)

save(data.both.FF.m.full, file = paste(filepath, "data.both.FF.m.full.RData", sep = ""))

data.both.FF.m <- data.both.FF.m.full %>%
  mutate_at(vars(d.shares:lag.AstChg), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
  select(Yearmon, datadate, PERMNO, shrout, naicsh, exchcd, prc, vol, retadj.1mn, d.shares, ME, port.weight, # [CK] REMOVED comp.count
         ret.12t2, at:AstChg, ME.Jun:lag.AstChg) %>%
  arrange(Yearmon, PERMNO) %>%
  group_by(PERMNO) %>%
  mutate_at(vars(ME.Jun:CFP.FF, lag.ME.Jun:lag.AstChg), list(~ na_locf_until(., 11))) %>%
  ungroup %>%
  mutate(port.weight = ifelse(is.na(port.weight), 0, port.weight)) # necessary to avoid NAs for weighted ret calc

# Unit test
# data.both.FF.m.validation <- summary(data.both.FF.m)
# save(data.both.FF.m.validation, file = "data.both.FF.m.validation.RData")
load("data.both.FF.m.validation.RData")
expect_equal(summary(data.both.FF.m), data.both.FF.m.validation)
rm(data.both.FF.m.validation)

save(data.both.FF.m, file = paste(filepath, "data.both.FF.m.RData", sep = ""))
rm(data.both.m, data.both.FF.m.full)

# FF Recon ####

# Form 2x3 FF Factors ##

Form_CharSizePorts2 <- function(main, size, var, daily) { # streamlined version
  # forms 2x3 (size x specificed-characteristc) and forms the 6 portfolios
  # variable broken by 30-70 percentiles, size broken up at 50 percentile (breakpoints uses NYSE data only)
  # requires Date and exchcd
  # outputs portfolio returns for each period,
  # browser()
  main.cln <- main %>%
    select(Date, PERMNO, exchcd, !!size, !!var)
  
  Bkpts.NYSE <- main.cln %>% # create size and var breakpoints based on NYSE stocks only
    filter(exchcd  ==  1) %>% # NYSE exchange
    group_by(Date) %>%
    summarize(var.P70 = quantile(!!var, probs = .7, na.rm = TRUE),
              var.P30 = quantile(!!var, probs = .3, na.rm = TRUE),
              size.Med = quantile(!!size, probs = .5, na.rm = TRUE))
  
  # calculate size and var portfolio returns
  main.rank <- main.cln %>%
    merge(Bkpts.NYSE, by = "Date", all.x = TRUE) %>%
    mutate(Size = ifelse(!!size < size.Med, "Small", "Big"),
           Var = ifelse(!!var < var.P30, "Low", ifelse(!!var > var.P70, "High", "Neutral")),
           Port = paste(Size, Var, sep = "."))
  
  df <- daily %>%
    select(Day, Date, PERMNO, port.weight, retadj.1mn) %>%
    merge(main.rank, by = c("Date", "PERMNO"), all.x = TRUE) %>%
    group_by(PERMNO) %>%    
    mutate_at(vars(Port), list(~ na.locf(., na.rm = FALSE)))
  
  Ret <- df %>% # name 2 x 3 size-var portfolios
    group_by(Day, Port) %>%
    summarize(ret.port = weighted.mean(retadj.1mn, port.weight, na.rm = TRUE)) %>% # calc value-weighted returns
    spread(Port, ret.port) %>% # transpose portfolios expressed as rows into seperate columns
    mutate(Small = (Small.High + Small.Neutral + Small.Low)/3,
           Big = (Big.High + Big.Neutral + Big.Low)/3,
           SMB = Small - Big,
           High = (Small.High + Big.High)/2,
           Low = (Small.Low + Big.Low)/2,
           HML = High - Low)
  
  return(Ret)
}

Form_FF6Ports <- function(monthly, daily) {
  # form FF6 factors from data (SMB, HML, RMW, CMA, UMD)
  # browser()
  output <- daily %>%
    group_by(Day) %>%
    summarize(MyMkt = weighted.mean(retadj.1mn, w = port.weight, na.rm = TRUE)) %>%
    merge(Form_CharSizePorts2(monthly, quo(lag.ME.Jun), quo(lag.BM.FF),daily),
          by = "Day", all.x = TRUE) %>% # SMB, HML
    select(Day:MyMkt, MySMB = SMB, MySMBS = Small, MySMBB = Big, MyHML = HML, MyHMLH = High, MyHMLL = Low) %>%
    merge(Form_CharSizePorts2(monthly, quo(lag.ME.Jun), quo(lag.OpIB), daily),
          by = "Day", all.x = TRUE) %>% # RMW
    select(Day:MyHMLL, MyRMW = HML, MyRMWR = High, MyRMWW = Low) %>%
    merge(Form_CharSizePorts2(monthly, quo(lag.ME.Jun), quo(lag.AstChg), daily),
          by = "Day", all.x = TRUE) %>% # CMA
    select(Day:MyRMWW, MyCMA = HML, MyCMAC = Low, MyCMAA = High) %>%
    mutate(MyCMA = -MyCMA) %>%
    merge(Form_CharSizePorts2(monthly, quo(lag.ME.Jun), quo(lag.ret.12t2), daily), 
          by = "Day", all.x = TRUE) %>% # UMD
    select(Day:MyCMAA, MyUMD = HML, MyUMDU = High, MyUMDD = Low)
  return(output)
}

load(paste(filepath, "data.both.FF.m.RData", sep = ""))
load(paste(filepath, "data.crsp.cln.d.RData", sep = ""))

start <- 1997
end <- 2018

data.both.FF.m.unlim <- data.both.FF.m
data.both.FF.m <- data.both.FF.m %>% 
  filter(year(Yearmon) > start & year(Yearmon) < end) %>% 
  mutate(Date = Yearmon)
save(data.both.FF.m, file = paste(filepath, "data.both.FF.m.RData", sep = ""))
save(data.both.FF.m.unlim, file = paste(filepath, "data.both.FF.m.unlim.RData", sep = ""))

data.crsp.cln.d.unlim <- data.crsp.cln.d
data.crsp.cln.d <- data.crsp.cln.d %>% 
  filter(year(Day) > start & year(Day) < end) %>% 
  mutate(Date = Day)
save(data.crsp.cln.d, file = paste(filepath, "data.crsp.cln.d.RData", sep = ""))
save(data.crsp.cln.d.unlim, file = paste(filepath, "data.crsp.cln.d.unlim.RData", sep = ""))

dt.myFF6.d <- Form_FF6Ports(data.both.FF.m, data.crsp.cln.d) 

save(dt.myFF6.d, file = paste(filepath, "dt.myFF6.d.RData", sep = ""))
rm(data.both.FF.m, data.crsp.cln.d, data.both.FF.m.unlim, data.crsp.cln.d.unlim)

# Load FF data - Run on local machine ####
library(dplyr)
library(zoo)
setwd("F:/Studium/PhD/HSG/Research/03_FactorModel/3rd R")
wb.daily <- read.csv("F-F_Research_Data_5_Factors_2x3_daily.csv")
save("wb.daily", file = "wb.daily.RDATA")

# Copy wb file into wrds directory, rename "RDATA" to "RData" ####

# Import FF6 and Mkt to check data - Run on WRDS Cloud ####
load(paste(filepath, "wb.daily.RData", sep = ""))

dt.FF6.d <- wb.daily %>%
  mutate(Day = ymd(Date),
         Mkt = Mkt.RF + RF) %>%
  mutate_at(vars(-Day, -Date), list(~ ./100)) %>%
  select(-Date) %>%
  arrange(Day) 

dt.FF6.d <- dt.FF6.d %>% filter(year(Day) >=  start & year(Day) <=  end)

save(dt.FF6.d, file = paste(filepath, "dt.FF6.d.RData", sep = ""))
rm(wb.daily)

# Check FF6 factor returns (Jul '26 through Dec '17) ####

Compare_Two_Vectors <- function(v1, v2, sqnc = 1) { # omits DATE option
  # takes two dataframes where each has a Date and another var column
  # compares the two var columns for similarity
  # v1.date, v2.date denotes col name of date which must be yearmon
  # sqnc affects the plot, e.g. if  = 3 then plot every third point (so not too crowded)
  # will not work if missing observations in between for one dataframe since cor needs exact same length
  
  # obtain common time segment among two vectors
  lo.date <- max(v1[["Day"]][min(which(!is.na(v1[[colnames(v1)[2]]])))],
                 v2[["Day"]][min(which(!is.na(v2[[colnames(v2)[2]]])))])
  hi.date <- min(v1[["Day"]][max(which(!is.na(v1[[colnames(v1)[2]]])))],
                 v2[["Day"]][max(which(!is.na(v2[[colnames(v2)[2]]])))])
  v1.trim <- subset(v1, lo.date <=  Day & Day <=  hi.date)
  v2.trim <- subset(v2, lo.date <=  Day & Day <=  hi.date)
  
  df <- v1.trim %>%
    merge(v2.trim, by = "Day", all.x = TRUE)
  
  print("correlation")
  print(cor(df[[colnames(df)[2]]], df[[colnames(df)[3]]], use = "complete.obs"))
  print("v1.trim mean")
  print(mean(df[[colnames(df)[2]]], na.rm = TRUE))
  print("v2.trim mean")
  print(mean(df[[colnames(df)[3]]], na.rm = TRUE))
  print("means: v1.trim-v2.trim")
  print(mean(df[[colnames(df)[2]]], na.rm = TRUE) - mean(df[[colnames(df)[3]]], na.rm = TRUE))
  # plot(df[["Date"]][seq(1,nrow(df),sqnc)],
  #      df[[colnames(df)[2]]][seq(1,nrow(df),sqnc)], type = "l", xlab = "Day", ylab = "Returns (%)")
  # lines(df[["Date"]][seq(1,nrow(df),sqnc)],
  #       df[[colnames(df)[3]]][seq(1,nrow(df),sqnc)], type = "l", col = "blue")
  # legend("topright", c("1", "2"), lty = 1, col = c("black","blue"))
}

load(paste(filepath, "dt.myFF6.d.RData", sep = ""))
load(paste(filepath, "dt.FF6.d.RData", sep = ""))

Compare_Two_Vectors(select(dt.myFF6.d, Day, MyMkt), select(dt.FF6.d, Day, Mkt), sqnc = 12)
Compare_Two_Vectors(select(dt.myFF6.d, Day, MySMB), select(dt.FF6.d, Day, SMB), sqnc = 12)
Compare_Two_Vectors(select(dt.myFF6.d, Day, MyHML), select(dt.FF6.d, Day, HML), sqnc = 12)
Compare_Two_Vectors(select(dt.myFF6.d, Day, MyRMW), select(dt.FF6.d, Day, RMW), sqnc = 12)
Compare_Two_Vectors(select(dt.myFF6.d, Day, MyCMA), select(dt.FF6.d, Day, CMA), sqnc = 12)
Compare_Two_Vectors(select(dt.myFF6.d, Day, MyUMD), select(dt.FF6.d, Day, UMD), sqnc = 12)

# # # # # # # # # # ####
# FF Recon MONTHLY ####

# Form 2x3 FF Factors ##

Form_CharSizePorts2 <- function(main, size, var, wght, ret) { # streamlined version
  # forms 2x3 (size x specificed-characteristc) and forms the 6 portfolios
  # variable broken by 30-70 percentiles, size broken up at 50 percentile (breakpoints uses NYSE data only)
  # requires Date and exchcd
  # outputs portfolio returns for each period,
  # browser()
  main.cln <- main %>%
    select(Date, PERMNO, exchcd, !!size, !!var, !!wght, !!ret)
  
  Bkpts.NYSE <- main.cln %>% # create size and var breakpoints based on NYSE stocks only
    filter(exchcd  ==  1) %>% # NYSE exchange
    group_by(Date) %>%
    summarize(var.P70 = quantile(!!var, probs = .7, na.rm = TRUE),
              var.P30 = quantile(!!var, probs = .3, na.rm = TRUE),
              size.Med = quantile(!!size, probs = .5, na.rm = TRUE))
  
  test <- main.cln %>%
    filter(exchcd  ==  1) %>%
    group_by(Date) %>%
    summarize(mean = mean(!!var, na.rm = TRUE))
  
  # calculate size and var portfolio returns
  main.rank <- main.cln %>%
    merge(Bkpts.NYSE, by = "Date", all.x = TRUE) %>%
    mutate(Size = ifelse(!!size<size.Med, "Small", "Big"),
           Var = ifelse(!!var<var.P30, "Low", ifelse(!!var>var.P70, "High", "Neutral")),
           Port = paste(Size, Var, sep = "."))
  
  Ret <- main.rank %>% # name 2 x 3 size-var portfolios
    group_by(Date, Port) %>%
    summarize(ret.port = weighted.mean(!!ret, !!wght, na.rm = TRUE)) %>% # calc value-weighted returns
    spread(Port, ret.port) %>% # transpose portfolios expressed as rows into seperate columns
    mutate(Small = (Small.High + Small.Neutral + Small.Low)/3,
           Big = (Big.High + Big.Neutral + Big.Low)/3,
           SMB = Small - Big,
           High = (Small.High + Big.High)/2,
           Low = (Small.Low + Big.Low)/2,
           HML = High - Low)
  
  return(Ret)
}

Form_FF6Ports <- function(df) {
  # form FF6 factors from data (SMB, HML, RMW, CMA, UMD)
  #browser()
  output <- df %>%
    group_by(Date) %>%
    summarize(MyMkt = weighted.mean(retadj.1mn, w = port.weight, na.rm = TRUE)) %>%
    merge(Form_CharSizePorts2(df, quo(lag.ME.Jun), quo(lag.BM.FF), quo(port.weight), quo(retadj.1mn)),
          by = "Date", all.x = TRUE) %>% # SMB, HML
    select(Date:MyMkt, MySMB = SMB, MySMBS = Small, MySMBB = Big, MyHML = HML, MyHMLH = High, MyHMLL = Low) %>%
    merge(Form_CharSizePorts2(df, quo(lag.ME.Jun), quo(lag.OpIB), quo(port.weight), quo(retadj.1mn)),
          by = "Date", all.x = TRUE) %>% # RMW
    select(Date:MyHMLL, MyRMW = HML, MyRMWR = High, MyRMWW = Low) %>%
    merge(Form_CharSizePorts2(df, quo(lag.ME.Jun), quo(lag.AstChg), quo(port.weight), quo(retadj.1mn)),
          by = "Date", all.x = TRUE) %>% # CMA
    select(Date:MyRMWW, MyCMA = HML, MyCMAC = Low, MyCMAA = High) %>%
    mutate(MyCMA = -MyCMA) %>%
    merge(Form_CharSizePorts2(df, quo(port.weight), quo(lag.ret.12t2), quo(port.weight), quo(retadj.1mn)), 
          by = "Date", all.x = TRUE) %>% # UMD
    select(Date:MyCMAA, MyUMD = HML, MyUMDU = High, MyUMDD = Low)
  return(output)
}

load(paste(filepath, "data.both.FF.m.RData", sep = ""))

start <- 1998
end <- 2018

dt.myFF6.m <- data.both.FF.m %>%
  rename(Date = Yearmon) %>%
  Form_FF6Ports %>%
  filter(year(Date) > start & year(Date) < end)

save(dt.myFF6.m, file = paste(filepath, "dt.myFF6.m.RData", sep = ""))

# Load FF data - Run on local machine ####
library(xlsx)
library(dplyr)
library(zoo)
setwd("F:/Studium/PhD/HSG/Research/03_FactorModel/3rd R")
wb <- read.xlsx("Fama French Factors Data.xlsx", 1)
save("wb", file = "wb.RDATA", ascii = TRUE, compress = FALSE)

# Copy wb file into wrds directory, rename "RDATA" to "RData" ####
# Import FF6 and Mkt to check data - Run on WRDS Cloud ####
load(paste(filepath, "wb.RData", sep = ""))

dt.FF6.m <- wb %>%
  mutate(Date = as.yearmon(ymd(paste0(Date,28))),
         Mkt = MktRF + RF) %>%
  mutate_at(vars(-Date), list( ~ ./100)) %>%
  arrange(Date) 

start <- 1998
end <- 2018

dt.FF6.m <- dt.FF6.m %>% 
  filter(year(Date) >=  start & year(Date) <=  end)

save(dt.FF6.m, file = paste(filepath, "dt.FF6.m.RData", sep = ""))

# Check FF6 factor returns (Jul '26 through Dec '17) ####

Compare_Two_Vectors3 <- function(v1, v2, sqnc = 1) { # omits DATE option
  # takes two dataframes where each has a Date and another var column
  # compares the two var columns for similarity
  # v1.date, v2.date denotes col name of date which must be yearmon
  # sqnc affects the plot, e.g. if  = 3 then plot every third point (so not too crowded)
  # will not work if missing observations in between for one dataframe since cor needs exact same length
  
  # obtain common time segment among two vectors
  lo.date <- max(v1[["Date"]][min(which(!is.na(v1[[colnames(v1)[2]]])))],
                 v2[["Date"]][min(which(!is.na(v2[[colnames(v2)[2]]])))])
  hi.date <- min(v1[["Date"]][max(which(!is.na(v1[[colnames(v1)[2]]])))],
                 v2[["Date"]][max(which(!is.na(v2[[colnames(v2)[2]]])))])
  v1.trim <- subset(v1, lo.date <= Date & Date <= hi.date)
  v2.trim <- subset(v2, lo.date <= Date & Date <= hi.date)
  
  df <- v1.trim %>%
    merge(v2.trim, by = "Date", all.x = TRUE)
  
  print("correlation")
  print(cor(df[[colnames(df)[2]]], df[[colnames(df)[3]]], use = "complete.obs"))
  print("v1.trim mean")
  print(mean(df[[colnames(df)[2]]], na.rm = TRUE))
  print("v2.trim mean")
  print(mean(df[[colnames(df)[3]]], na.rm = TRUE))
  print("means: v1.trim-v2.trim")
  print(mean(df[[colnames(df)[2]]], na.rm = TRUE) - mean(df[[colnames(df)[3]]], na.rm = TRUE))
  plot(df[["Date"]][seq(1,nrow(df),sqnc)],
       df[[colnames(df)[2]]][seq(1,nrow(df),sqnc)], type = "l", xlab = "Date", ylab = "Returns (%)")
  lines(df[["Date"]][seq(1,nrow(df),sqnc)],
        df[[colnames(df)[3]]][seq(1,nrow(df),sqnc)], type = "l", col = "blue")
  legend("topright", c("1", "2"), lty = 1, col = c("black","blue"))
}

load(paste(filepath, "dt.myFF6.m.RData", sep = ""))
load(paste(filepath, "dt.FF6.m.RData", sep = ""))

Compare_Two_Vectors3(select(dt.myFF6.m, Date, MyMkt), select(dt.FF6.m, Date, Mkt), sqnc = 12)
Compare_Two_Vectors3(select(dt.myFF6.m, Date, MySMB), select(dt.FF6.m, Date, SMB), sqnc = 12)
Compare_Two_Vectors3(select(dt.myFF6.m, Date, MyHML), select(dt.FF6.m, Date, HML), sqnc = 12)
Compare_Two_Vectors3(select(dt.myFF6.m, Date, MyRMW), select(dt.FF6.m, Date, RMW), sqnc = 12)
Compare_Two_Vectors3(select(dt.myFF6.m, Date, MyCMA), select(dt.FF6.m, Date, CMA), sqnc = 12)
Compare_Two_Vectors3(select(dt.myFF6.m, Date, MyUMD), select(dt.FF6.m, Date, UMD), sqnc = 12)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Add Sentiment ####
# Load matching file - run locally ####
setwd("F:/Studium/PhD/HSG/Research/03_FactorModel/3rd R")
ticker.PERMNO.match <- read.csv("ticker.PERMNO.match.csv")
save(ticker_PERMNO_match, file = "F:/Studium/PhD/HSG/Research/03_FactorModel/3rd R/ticker.PERMNO.match.RData")


# Copy ticker.PERMNO.match.RData file to WRDS ####


# Load Sentiment Data - run locally ####

# Define connection string to the SQL Server instance
sqlConnString <- "Driver = SQL Server;Server = localhost; Database = PhD;trusted_connection = true"

# Define number of rows for chunk processing
sqlRowsPerRead = 10000

# Set path 
path <- "F:/Studium/PhD/HSG/Research/03_FactorModel/3rd R/"

# Create an xdf file name
localXdfFileName <- file.path(path, "TRMI.xdf")

# Construct query
TRMI.query = paste("SELECT ticker, windowTimestamp, buzz, sentiment FROM MarketPsych.dbo.CMPNY_UDAI where dataType = 'News_Social'", sep = "")

# Construct path
TRMI.path <- RxSqlServerData(
  connectionString = sqlConnString,
  sqlQuery = TRMI.query,
  rowsPerRead = sqlRowsPerRead
)

# Import the news and social media data into a xdf file
TRMI.xdf <-
  rxImport(
    TRMI.path,
    #    "TRMI.xdf",
    overwrite = TRUE,
    transforms = list(
      windowTimestamp = as.POSIXct(windowTimestamp, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "GMT"),
      buzz = as.numeric(buzz),
      sentiment = as.numeric(sentiment)
    ),
    append  = 'none',
    rowsPerRead = sqlRowsPerRead
  )
save(TRMI.xdf, file = "TRMI.RData")


# Load Full Sentiment Data - run locally ####

# Define connection string to the SQL Server instance
sqlConnString <- "Driver = SQL Server;Server = localhost; Database = PhD;trusted_connection = true"

# Define number of rows for chunk processing
sqlRowsPerRead = 10000

# Set path 
path <- "F:/Studium/PhD/HSG/Research/03_FactorModel/3rd R"

# Create an xdf file name
localXdfFileName <- file.path(path, "TRMI.xdf")

# Construct query
TRMI.query = paste("SELECT ticker, windowTimestamp, buzz, sentiment, optimism, joy, loveHate, trust, anger, conflict, fear, gloom, stress, surprise, timeUrgency, uncertainty, violence, emotionVsFact, marketRisk, longShort, longShortForecast, priceDirection, priceForecast, volatility, analystRating, debtDefault, dividends, innovation, earningsForecast, fundamentalStrength, laborDispute, layoffs, litigation, managementChange, managementTrust, mergers, cyberCrime FROM MarketPsych.dbo.CMPNY_UDAI where dataType = 'News_Social'", sep = "")

# Construct path
TRMI.path <- RxSqlServerData(
  connectionString = sqlConnString,
  sqlQuery = TRMI.query,
  rowsPerRead = sqlRowsPerRead
)

# Import the news and social media data into a xdf file
TRMI.xdf <-
  rxImport(
    TRMI.path,
    #    "TRMI.xdf",
    overwrite = TRUE,
    transforms = list(
      windowTimestamp = as.POSIXct(windowTimestamp, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "GMT"),
      buzz = as.numeric(buzz),
      optimism = as.numeric(optimism),
      joy = as.numeric(joy),
      loveHate = as.numeric(loveHate),
      trust = as.numeric(trust),
      anger = as.numeric(anger),
      conflict = as.numeric(conflict),
      fear = as.numeric(fear),
      gloom = as.numeric(gloom),
      stress = as.numeric(stress),
      surprise = as.numeric(surprise),
      timeUrgency = as.numeric(timeUrgency),
      uncertainty = as.numeric(uncertainty),
      violence = as.numeric(violence),
      emotionVsFact = as.numeric(emotionVsFact),
      marketRisk = as.numeric(marketRisk),
      longShort = as.numeric(longShort),
      longShortForecast = as.numeric(longShortForecast),
      priceDirection = as.numeric(priceDirection),
      priceForecast = as.numeric(priceForecast),
      volatility = as.numeric(volatility),
      debtDefault = as.numeric(debtDefault),
      dividends = as.numeric(dividends),
      innovation = as.numeric(innovation),
      earningsForecast = as.numeric(earningsForecast),
      fundamentalStrength = as.numeric(fundamentalStrength),
      laborDispute = as.numeric(laborDispute),
      layoffs = as.numeric(layoffs),
      litigation = as.numeric(litigation),
      managementChange = as.numeric(managementChange),
      managementTrust = as.numeric(managementTrust),
      mergers = as.numeric(mergers),
      cyberCrime = as.numeric(cyberCrime)
      ),
    append  = 'none',
    rowsPerRead = sqlRowsPerRead
  )
save(TRMI.xdf, file = "TRMI.full.RData")


# Copy TRMI.RData and FF5 data file to WRDS ####


# Load TRMI file - run on WRDS ####
snt.selector <- 1 #1:social, 2:news, 3:news_social

if (snt.selector  ==  1) {
  load("TRMI_Social.RData")
} else if (snt.selector  ==  2) {
  load("TRMI_News.RData")
} else if (snt.selector  ==  3) {
  load("TRMI_Newssocial.RData")
}

load(paste(filepath, "ticker.PERMNO.match.RData", sep = ""))
ticker.PERMNO.match <- ticker_PERMNO_match %>%
  mutate(PERMNO = as.factor(LPERMNO),
         LINKDT = as.Date(as.numeric(LINKDT)),
         LINKENDDT = as.Date(LINKENDDT, format = "%Y%m%d"),
         LINKTYPE = factor(LINKTYPE, levels = c("LC", "LU", "LS")),
         LINKPRIM = factor(LINKPRIM, levels = c("P", "C", "J")))

# Add identifiers to TRMI
TRMI <- TRMI.xdf %>%
  select(windowTimestamp, ticker, sentiment, buzz) %>%
  merge(ticker.PERMNO.match, by.x = "ticker", by.y = "tic") %>% # inner join
  mutate(Day = as.Date(windowTimestamp)) %>%
  filter(Day >=  LINKDT & (Day <=  LINKENDDT | is.na(LINKENDDT))) %>%
  # prioritize linktype, linkprim based on order of preference/primary if duplicate
  arrange(Day, PERMNO, LINKTYPE, LINKPRIM) %>%
  dplyr::distinct(Day, PERMNO, .keep_all = TRUE)

save(TRMI, file = paste(filepath, "TRMI.RData", sep = ""))
load(paste(filepath, "TRMI.RData", sep = ""))

Fill_TS_NAs_TRMI <- function(main, window) {
  # takes datat frame with Date and PERMNO as columns and fills in NAs where there are gaps
  #browser()
  core <- select(main, !!window, PERMNO)
  # find first and last dates of each PERMNO
  date.bookends <- core %>%
    group_by(PERMNO) %>%
    summarize(first = first(!!window), last = last(!!window))
  
  # generate all dates for all PERMNOs then trim those outside of each PERMNO's first and last dates
  output <- core %>%
    mutate(temp = 1) %>% # create 3rd column so spread can be applied
    spread(., PERMNO, temp) %>%
    gather(., PERMNO, temp, -!!window) %>%
    left_join(date.bookends, by = "PERMNO") %>%
    group_by(PERMNO) %>%
    filter(!!window >= first & !!window <= last) %>%
    select(!!window, PERMNO) %>%
    merge(., main, by = c(quo_name(window), "PERMNO"), all.x = T) %>%
    mutate(PERMNO = as.factor(PERMNO)) 
  
  return(output)
}

# Function to mainpulate daily TRMI data
TRMI_man <- function(df){
   # browser()
  return( df %>% 
            filter(!is.na(LPERMNO)) %>% # only where PERMNO is not missing
            #Fill_TS_NAs_TRMI(quo(Day)) %>%
            group_by(PERMNO) %>%
            arrange(Day) %>%
            mutate_at(vars(sentiment), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
            mutate_at(vars(sentiment), list(~ ifelse(!is.nan(.), ., NA))) %>% # code NAN values as NAs
            #mutate(snt = na.locf0(sentiment)) %>% #na.locf0 / replace_na(sentiment, 0)
            rename(snt = sentiment) %>% # added if previous was commented out
            select(PERMNO, Day, snt) %>%
            ungroup
  )
}

# Function to aggregate TRMI data to monthly
TRMI_aggr_level <- function(df){
  return( df %>% 
            filter(!is.na(LPERMNO)) %>% # only where PERMNO is not missing
            mutate(Yearmon = as.yearmon(as.Date(windowTimestamp))) %>% 
            arrange(PERMNO, Yearmon) %>%
            group_by(PERMNO, Yearmon) %>% 
            summarize(snt = weighted.mean(sentiment, buzz, na.rm = TRUE)) %>%
            #Fill_TS_NAs_TRMI(quo(Yearmon)) %>%
            group_by(PERMNO) %>%
            arrange(Yearmon) %>%
            mutate_at(vars(snt), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
            mutate_at(vars(snt), list(~ ifelse(!is.nan(.), ., NA))) %>% # code NAN values as NAs
            #mutate(snt = replace_na(snt, 0)) %>% #na.locf0
            select(PERMNO, Yearmon, snt) %>% 
            ungroup
  )
}

# Function to aggregate TRMI data to weekly
TRMI_aggr_level_w <- function(df){
    return( df %>% 
            filter(!is.na(LPERMNO),
                   !weekdays(Day) %in% c("Saturday", "Sunday")) %>% # only where PERMNO is not missing, remove weekends
            mutate(Yearweek = yearweek(as.Date(windowTimestamp))) %>%
            arrange(PERMNO, Yearweek)  %>%
            group_by(PERMNO, Yearweek) %>% 
            summarize(snt = weighted.mean(sentiment, buzz, na.rm = TRUE)) %>%
            #Fill_TS_NAs_TRMI(quo(Yearweek)) %>%  
            group_by(PERMNO) %>%
            arrange(Yearweek) %>%
            mutate_at(vars(snt), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
            mutate_at(vars(snt), list(~ ifelse(!is.nan(.), ., NA))) %>% # code NAN values as NAs
            #mutate(snt = na.locf0(snt)) %>% #na.locf0 / replace_na(snt, 0) / 
            select(PERMNO, Yearweek, snt) %>% 
            ungroup
    )
}

# Function to get change of sentiment for daily
TRMI_aggr_diff_d <- function(df){
  return( df %>% 
            TRMI_man %>% # aggregate data to monthly
            arrange(PERMNO, Day) %>%
            group_by(PERMNO) %>%
            mutate(snt.diff = snt - lag(snt)) %>% # c(NA,diff(snt))) %>%
            mutate_at(vars(snt.diff), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
            mutate_at(vars(snt.diff), list(~ ifelse(!is.nan(.), ., NA))) %>% # code NAN values as NAs
            select(-snt) %>%
            ungroup
  )
}

# Function to get change of sentiment for monthly
TRMI_aggr_diff_m <- function(df){
  return( df %>% 
            TRMI_aggr_level %>% 
            arrange(PERMNO, Yearmon) %>%
            group_by(PERMNO) %>%
            mutate(snt.diff = snt - lag(snt)) %>% # c(NA,diff(log(aggr.snt)))) %>%
            mutate_at(vars(snt.diff), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
            mutate_at(vars(snt.diff), list(~ ifelse(!is.nan(.), ., NA))) %>% # code NAN values as NAs
            select(-snt) %>%
            ungroup
  )
}

# Function to get change of sentiment for weekly
TRMI_aggr_diff_w <- function(df){
  return( df %>% 
            TRMI_aggr_level_w %>% 
            arrange(PERMNO, Yearweek) %>%
            group_by(PERMNO) %>%
            mutate(snt.diff = snt - lag(snt)) %>% # c(NA,diff(log(aggr.snt)))) %>%
            mutate_at(vars(snt.diff), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
            mutate_at(vars(snt.diff), list(~ ifelse(!is.nan(.), ., NA))) %>% # code NAN values as NAs
            select(-snt) %>%
            ungroup
  )
}

# Function to get global change of sentiment for daily
TRMI_aggr_glo_diff_d <- function(df){ 
  return( df %>% 
            TRMI_man %>% # aggregate data to monthly
            group_by(PERMNO) %>%
            mutate(snt.glo.diff = snt - mean(snt, na.rm = TRUE)) %>%
            mutate_at(vars(snt.glo.diff), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
            mutate_at(vars(snt.glo.diff), list(~ ifelse(!is.nan(.), ., NA))) %>% # code NAN values as NAs
            select(-snt) %>%
            ungroup
  )
}

# Function to get global change of sentiment for monthly
TRMI_aggr_glo_diff_m <- function(df){ 
  return( df %>% 
            TRMI_aggr_level %>% # aggregate data to monthly
            group_by(PERMNO) %>%
            mutate(snt.glo.diff = snt - mean(snt, na.rm = TRUE)) %>%
            mutate_at(vars(snt.glo.diff), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
            mutate_at(vars(snt.glo.diff), list(~ ifelse(!is.nan(.), ., NA))) %>% # code NAN values as NAs
            select(-snt) %>%
            ungroup
  )
}

# Function to get global change of sentiment for weekly
TRMI_aggr_glo_diff_w <- function(df){ 
  return( df %>% 
            TRMI_aggr_level_w %>% 
            group_by(PERMNO) %>%
            mutate(snt.glo.diff = snt - mean(snt, na.rm = TRUE)) %>%
            mutate_at(vars(snt.glo.diff), list(~ ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
            mutate_at(vars(snt.glo.diff), list(~ ifelse(!is.nan(.), ., NA))) %>% # code NAN values as NAs
            select(-snt) %>%
            ungroup
  )
}

TRMI.aggr.d <- TRMI %>%
  TRMI_man %>%
  join(TRMI_aggr_diff_d(TRMI), by = c("PERMNO", "Day")) %>%
  join(TRMI_aggr_glo_diff_d(TRMI), by = c("PERMNO", "Day")) %>%
  arrange(PERMNO, Day) 

save(TRMI.aggr.d, file = paste(filepath, "TRMI.aggr.d.RData", sep = ""))

TRMI.aggr.m <- TRMI %>%
  TRMI_aggr_level %>%
  join(TRMI_aggr_diff_m(TRMI), by = c("PERMNO", "Yearmon")) %>%
  join(TRMI_aggr_glo_diff_m(TRMI), by = c("PERMNO", "Yearmon")) %>%
  arrange(PERMNO, Yearmon) 

save(TRMI.aggr.m, file = paste(filepath, "TRMI.aggr.m.RData", sep = ""))

TRMI.aggr.w <- TRMI %>%
  TRMI_aggr_level_w %>%
  join(TRMI_aggr_diff_w(TRMI), by = c("PERMNO", "Yearweek")) %>%
  join(TRMI_aggr_glo_diff_w(TRMI), by = c("PERMNO", "Yearweek")) %>%
  arrange(PERMNO, Yearweek) 

save(TRMI.aggr.w, file = paste(filepath, "TRMI.aggr.w.RData", sep = ""))
rm(snt.selector, TRMI, TRMI.xdf, ticker.PERMNO.match)

ana <- function(df){
  print(dim(df))
  print(colnames(df))
  print(str(df))
  print(head(as.data.frame(filter(df, PERMNO == 17144))))
  print(summary(df))
}
save(ana, file = paste(filepath, "analyze_objects.RData", sep = ""))

# Merge FF5 and Sentiment data ####

start <- 1997 # greater than 1925 or 1997 or 1962
end <- 2018 # smaller than 2018 or 2014

# daily
load(paste(filepath, "data.crsp.cln.d.RData", sep = ""))
load(paste(filepath, "TRMI.aggr.d.RData", sep = ""))
data.snt.d <- data.crsp.cln.d %>%
  filter(year(Day) > start & year(Day) < end) %>%
  left_join(TRMI.aggr.d, by = c("PERMNO", "Day")) %>%
  arrange(PERMNO, Day)

# Unit Test
# data.snt.d.validation <- summary(data.snt.d)
# save(data.snt.d.validation, file = "data.snt.d.validation.RData")
load("data.snt.d.validation.RData")
expect_equal(summary(data.snt.d), data.snt.d.validation)
rm(data.snt.d.validation)

save(data.snt.d, file = paste(filepath, "data.snt.d.RData", sep = ""))
rm(TRMI.aggr.d, data.crsp.cln.d)

# monthly
load(paste(filepath, "data.both.FF.m.RData", sep = ""))
load(paste(filepath, "TRMI.aggr.m.RData", sep = ""))
data.snt.m <- data.both.FF.m %>%
  filter(year(Yearmon) > start & year(Yearmon) < end) %>%
  left_join(TRMI.aggr.m, by = c("PERMNO", "Yearmon")) %>% 
  arrange(PERMNO, Yearmon)

# Unit Test
# data.snt.m.validation <- summary(data.snt.m)
# save(data.snt.m.validation, file = "data.snt.m.validation.RData")
load("data.snt.m.validation.RData")
expect_equal(summary(data.snt.m), data.snt.m.validation)
rm(data.snt.m.validation)

save(data.snt.m, file = paste(filepath, "data.snt.m.RData", sep = ""))
rm(TRMI.aggr.m, data.both.FF.m)

# weekly
load(paste(filepath, "data.crsp.cln.d.RData", sep = ""))
load(paste(filepath, "TRMI.aggr.w.RData", sep = ""))
load(paste(filepath, "data.snt.m.RData", sep = ""))

data.crsp.cln.w <- data.crsp.cln.d %>%
  filter(year(Day) > start & year(Day) < end) %>%
  mutate(Yearweek = yearweek(as.Date(Day))) %>%
  arrange(PERMNO, Yearweek) %>%
  group_by(PERMNO, Yearweek) %>% 
  summarize(retadj.1mn = prod(1 + retadj.1mn, na.rm = TRUE) - 1,
            port.weight = mean(port.weight, na.rm = TRUE)) %>% 
  ungroup

# data.snt.w %>% filter(PERMNO == 17144) %>% as.data.frame() %>% head(20)
# TRMI.aggr.d %>% filter(PERMNO == 17144) %>% as.data.frame() %>% head(20)

save(data.crsp.cln.w, file = paste(filepath, "data.crsp.cln.w.RData", sep = ""))

data.snt.w <- data.crsp.cln.w %>%
  left_join(TRMI.aggr.w, by = c("PERMNO", "Yearweek")) %>%
  arrange(PERMNO, Yearweek) %>%
  mutate(Yearmon = as.yearmon(as.Date(Yearweek)))

data.snt.w <- data.snt.m %>%  
  select(-c(snt, snt.diff, snt.glo.diff, retadj.1mn, port.weight)) %>%
  right_join(data.snt.w, by = c("PERMNO", "Yearmon")) %>%
  arrange(PERMNO, Yearweek)

# Unit Test
# data.snt.w.validation <- summary(data.snt.w)
# save(data.snt.w.validation, file = "data.snt.w.validation.RData")
load("data.snt.w.validation.RData")
expect_equal(summary(data.snt.w), data.snt.w.validation)
rm(data.snt.w.validation)

save(data.snt.w, file = paste(filepath, "data.snt.w.RData", sep = ""))
rm(TRMI.aggr.w, data.crsp.cln.d, data.snt.m)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Form Mimicking Portfolios ####
# Differentiate between daily or weekly or monthly data ####
start <- 1997 # greater than 1925 or 1997 or 1962
end <- 2018 # smaller than 2018 or 2014

# daily
load(paste(filepath, "dt.FF6.d.RData", sep = "" ))

rf <- dt.FF6.d %>% 
  select(Day, RF) %>%
  filter(year(Day) > start & year(Day) < end) %>%
  rename(Date = Day)

dt.FF6 <- dt.FF6.d %>%
  rename(Date = Day)

load(paste(filepath, "data.snt.d.RData", sep = ""))
load(paste(filepath, "data.snt.m.RData", sep = ""))

data.snt <- data.snt.m %>% 
  select( -c(exchcd, prc, vol, shrout, port.weight, ME, retadj.1mn, snt, snt.diff, snt.glo.diff)) %>%
  right_join(mutate(data.snt.d, Yearmon = as.yearmon(Day)), by = c("Yearmon", "PERMNO")) %>%
  arrange(PERMNO, Day) %>%
  rename(Date = Day)

# monthly
load(paste(filepath, "dt.FF6.m.RData", sep = "" ))
rf <- dt.FF6.m %>% 
  select(Date, RF) %>%
  filter(year(Date) > start & year(Date) < end) 

dt.FF6 <- dt.FF6.m

load(paste(filepath, "data.snt.m.RData", sep = ""))
data.snt <- data.snt.m %>%
  rename(Date = Yearmon)

# weekly
load(paste(filepath, "data.crsp.cln.d.RData", sep = ""))
load(paste(filepath, "TRMI.aggr.w.RData", sep = ""))
load(paste(filepath, "data.snt.m.RData", sep = ""))

data.crsp.cln.w <- data.crsp.cln.d %>%
  filter(year(Day) > start & year(Day) < end) %>%
  mutate(Yearweek = yearweek(as.Date(Day))) %>%
  arrange(PERMNO, Yearweek) %>%
  group_by(PERMNO, Yearweek) %>% 
  summarize(retadj.1mn = prod(1 + retadj.1mn, na.rm = TRUE) - 1,
            port.weight = mean(port.weight, na.rm = TRUE)) %>% 
  ungroup

save(data.crsp.cln.w, file = paste(filepath, "data.crsp.cln.w.RData", sep = ""))

data.snt.w <- data.crsp.cln.w %>%
  left_join(TRMI.aggr.w, by = c("PERMNO", "Yearweek")) %>%
  arrange(PERMNO, Yearweek) %>%
  mutate(Yearmon = as.yearmon(as.Date(Yearweek)))

data.snt.w <- data.snt.m %>%  
  select(-c(snt, snt.diff, snt.glo.diff, retadj.1mn, port.weight)) %>%
  right_join(data.snt.w, by = c("PERMNO", "Yearmon")) %>%
  mutate(Yearweek = yearweek(Yearweek)) %>% 
  arrange(PERMNO, Yearweek)

data.snt <- data.snt.w %>%
  dplyr::mutate(Date = Yearweek)

load(paste(filepath, "dt.FF6.d.RData", sep = "" ))
rf <- dt.FF6.d %>% 
  select(Day, RF) %>%
  filter(year(Day) > start & year(Day) < end) %>%
  mutate(Date = yearweek(as.Date(Day))) %>%
  group_by(Date) %>%
  summarize(RF = prod(1 + RF, na.rm = TRUE) - 1)

save(data.snt.w, file = paste(filepath, "data.snt.w.RData", sep = ""))
rm(TRMI.aggr.w, data.crsp.cln.d, data.snt.m)

# # # # #
# Lagged sentiment?
lag = 1 #Lag: 1, No lag: 0
save(lag, file = paste(filepath, "lag.RData", sep = ""))
data.snt <- data.snt %>% 
  mutate(snt = lag(snt, lag),
         snt.diff = lag(snt.diff, lag),
         snt.glo.diff = lag(snt.glo.diff, lag)) 

save(rf, file = paste(filepath, "rf.RData", sep = ""))
save(data.snt, file = paste(filepath, "data.snt.RData", sep = ""))
save(dt.FF6, file = paste(filepath, "dt.FF6.RData", sep = ""))

rm(data.snt.d, data.snt.m, dt.FF6.m, dt.FF6.d)

# Form 5x5 portfolios ####
load(paste(filepath, "data.snt.RData", sep = ""))
load(paste(filepath, "rf.RData", sep = ""))

# function to form sorts
Form_CharSizePorts2_snt_5x5_new <- function(main, size, var, rf) { # streamlined version
  # forms 5x5 (size x specificed-characteristc) and forms the 25 portfolios
  # variables broken by quantiles (breakpoints uses NYSE data only)
  # requires Date and exchcd
  # outputs portfolio *excess* returns for each period,
  # browser()
  main.cln <- main %>%
    select(Date, PERMNO, exchcd, !!size, !!var)
  
  Bkpts.NYSE <- main.cln %>% # create size and var breakpoints based on NYSE stocks only
    filter(exchcd  ==  1) %>% # NYSE exchange
    group_by(Date) %>%
    summarize(var.P80 = quantile(!!var, probs = .8, na.rm = TRUE),
              var.P60 = quantile(!!var, probs = .6, na.rm = TRUE),
              var.P40 = quantile(!!var, probs = .4, na.rm = TRUE),
              var.P20 = quantile(!!var, probs = .2, na.rm = TRUE),
              size.S80 = quantile(!!size, probs = .8, na.rm = TRUE),
              size.S60 = quantile(!!size, probs = .6, na.rm = TRUE),
              size.S40 = quantile(!!size, probs = .4, na.rm = TRUE),
              size.S20 = quantile(!!size, probs = .2, na.rm = TRUE))
  
  # calculate size and var portfolio returns
  main.rank <- main.cln %>%
    left_join(Bkpts.NYSE, by = "Date") %>% #format was lost due to join
    mutate(Date = yearweek(Date), #format was lost due to join
           Size = ifelse(!!size < size.S20, "S1", ifelse(!!size < size.S40, "S2", ifelse(!!size < size.S60, "S3", ifelse(!!size < size.S80, "S4", "S5")))), #small big
           Var = ifelse(!!var < var.P20, "P1", ifelse(!!var < var.P40, "P2", ifelse(!!var < var.P60, "P3", ifelse(!!var < var.P80, "P4", "P5"))))) %>% #low high
    group_by(PERMNO) %>% # *NEW
    mutate_at(vars(Size, Var), list(~ na.locf(., na.rm = FALSE))) %>%
    mutate(Port = paste(Size, Var, sep = ".")) %>%
    select(Date, PERMNO, Port)
  
  df <- main %>%
    select(Date, PERMNO, port.weight, retadj.1mn) %>%
    left_join(main.rank, by = c("Date", "PERMNO")) %>% 
    group_by(PERMNO) %>%    
    mutate(Date = yearweek(Date)) %>% #format was lost due to join,
    mutate_at(vars(Port), list(~ na.locf(., na.rm = FALSE))) 
  
  Ret <- df %>% 
    group_by(Date, Port) %>%
    summarize(ret.port = weighted.mean(coalesce(retadj.1mn,0), coalesce(port.weight,0), na.rm = TRUE)) %>% # calc value-weighted returns
    spread(key = Port, value = ret.port) %>%
    ungroup %>% # transpose portfolios expressed as rows into seperate columns
    plyr::join(rf, by = "Date") %>% 
    mutate(Date = yearweek(Date)) %>%  #format was lost due to join,
    mutate_at(vars(-Date), list(~ (. - RF))) %>% #compute excess returns
    select(-RF)
  
  return(Ret)
}

construct_portfolio <- function(df){
  # browser()
  output <- df %>% 
    ungroup %>%
    select(colnames(df[!grepl("NA", colnames(df))]))
}

construct_5x5_table <- function(df){
  # browser()
  rounder <- 10000 #1oo for %, 10000 for bps
  output <- df %>% 
    ungroup %>%
    select(colnames(df[!grepl("NA", colnames(df))]),-Date) %>%
    as.data.frame %>%
    colMeans(na.rm = TRUE) %>%
    matrix(.,nrow = 5, ncol = 5, byrow = TRUE)
  rownames(output) <- c("Small", "2", "3", "4", "Big")
  colnames(output) <- c("Negative", "2", "3", "4", "Positive")
  output *rounder #scale to bps
}

temp1 <- Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), rf)
temp2 <- Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(lag.OpIB), rf)
temp3 <- Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(lag.AstChg), rf)
temp4 <- Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(lag.ret.12t2), rf)
temp5 <- Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(snt.glo.diff), rf) #global difference
temp6 <- Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(snt.diff), rf) #change to previous day
temp7 <- Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(snt), rf) #sentiment level

list.5x5.raw.d <- list(temp1, temp2, temp3, temp4, temp5, temp6, temp7)
save(list.5x5.raw.d, file = paste(filepath, "list.5x5.raw.d.RData", sep = ""))
rm(temp1, temp2, temp3, temp4, temp5, temp6, temp7)

list.5x5.pf.d <- list.5x5.raw.d %>% lapply(construct_portfolio)
save(list.5x5.pf.d, file = paste(filepath, "list.5x5.pf.d.RData", sep = ""))

table_5x5.d <- list.5x5.raw.d %>% lapply(construct_5x5_table)
save(table_5x5.d, file = paste(filepath, "table_5x5.d.Rdata", sep = ""))

attr(table_5x5.d, "subheadings") <- c("Panel A: Size-B/M Portfolios",
                                      "Panel B: Size-OP Portfolios",
                                      "Panel C: Size-Inv Portfolios",
                                      "Panel D: Size-Mom Portfolios",
                                      "Panel E: Size-SNT Portfolios",
                                      "Reference Previous Day",
                                      "Reference Sentiment Level")

table_5x5.xtL.d <- table_5x5.d %>% 
  xtableList(colNames = FALSE,
             caption = "5x5 Portfolios",
             include.rownames = TRUE,
             digits = 2,
             label = "table_5x5.d")

save(table_5x5.xtL.d, file = paste(filepath, "table_5x5.xtL.d.RData", sep = ""))

print.xtableList(table_5x5.xtL.d,
                 tabular.environment = "tabularx",
                 file = "table_5x5.d.tex",
                 size = "footnotesize",
                 caption.placement = "top",
                 include.rownames = TRUE)

rm(list.5x5.raw.d, list.5x5.pf.d, table_5x5.d, table_5x5.xtL.d)

# Additional 5x5 portfolios with Sentiment ####
load(paste(filepath, "data.snt.RData", sep = ""))
load(paste(filepath, "rf.RData", sep = ""))

# load function from above

list.5x5.raw2.d <- list(Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(snt.glo.diff), rf),
                        Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.BM.FF), quo(snt.glo.diff), rf),
                        Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.OpIB), quo(snt.glo.diff), rf),
                        Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.AstChg), quo(snt.glo.diff), rf),
                        Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ret.12t2), quo(snt.glo.diff), rf),
                        
                        Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(snt.diff), rf),
                        Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.BM.FF), quo(snt.diff), rf),
                        Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.OpIB), quo(snt.diff), rf),
                        Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.AstChg), quo(snt.diff), rf),
                        Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ret.12t2), quo(snt.diff), rf)
                        
                        # Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ME.Jun), quo(snt), rf),
                        # Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.BM.FF), quo(snt), rf),
                        # Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.OpIB), quo(snt), rf),
                        # Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.AstChg), quo(snt), rf),
                        # Form_CharSizePorts2_snt_5x5_new(data.snt, quo(lag.ret.12t2), quo(snt), rf)
                        )

save(list.5x5.raw2.d, file = paste(filepath, "list.5x5.raw2.d.RData", sep = ""))

list.5x5.pf2.d <- list.5x5.raw2.d %>% lapply(construct_portfolio)
save(list.5x5.pf2.d, file = paste(filepath, "list.5x5.pf2.d.RData", sep = ""))

table_5x52.d <- list.5x5.raw2.d %>% lapply(construct_5x5_table)
save(table_5x52.d, file = paste(filepath, "table_5x52.d.Rdata", sep = ""))

attr(table_5x52.d, "subheadings") <- c("Panel A: Size-SNT Portfolios",
                                       "Panel B: B/M-SNT Portfolios",
                                       "Panel C: OP-SNT Portfolios",
                                       "Panel D: Inv-SNT Portfolios",
                                       "Panel E: Mom-SNT Portfolios",
                                       
                                       "Reference Previous Day: Size-SNT Portfolios",
                                       "Reference Previous Day: B/M-SNT Portfolios",
                                       "Reference Previous Day: OP-SNT Portfolios",
                                       "Reference Previous Day: Inv-SNT Portfolios",
                                       "Reference Previous Day: Mom-SNT Portfolios"
                                       
                                       # "Reference Sentiment Level: Size-SNT Portfolios",
                                       # "Reference Sentiment Level: B/M-SNT Portfolios",
                                       # "Reference Sentiment Level: OP-SNT Portfolios",
                                       # "Reference Sentiment Level: Inv-SNT Portfolios",
                                       # "Reference Sentiment Level: Mom-SNT Portfolios"
                                       )

table_5x5.xtL2.d <- table_5x52.d %>% 
  xtableList(colNames = FALSE,
             caption = "5x5 Portfolios",
             include.rownames = TRUE,
             digits = 2,
             label = "table_5x52.d")

save(table_5x5.xtL2.d, file = paste(filepath, "table_5x5.xtL2.d.RData", sep = ""))

print.xtableList(table_5x5.xtL2.d,
                 tabular.environment = "tabularx",
                 file = "table_5x52.d.tex",
                 size = "footnotesize",
                 caption.placement = "top",
                 include.rownames = TRUE)

rm(list.5x5.raw2.d, list.5x5.pf2.d, table_5x52.d, table_5x5.xtL2.d)

# Form 2x2x3x3 portfolios ####
load(paste(filepath, "data.snt.RData", sep = ""))
load(paste(filepath, "rf.RData", sep = ""))

Form_2x2x3x3_portfolios_new <- function(main, size, value, var3, snt, rf) { # streamlined version
  # forms 5x5 (size x specificed-characteristc) and forms the 25 portfolios
  # variables broken by quantiles (breakpoints uses NYSE data only)
  # requires Date and exchcd
  # outputs portfolio *excess* returns for each period,
  # browser()
  main.cln <- main %>%
    select(Date, PERMNO, exchcd, !!size, !!value, !!var3, !!snt)
  
  Bkpts.NYSE <- main.cln %>% # create size and var breakpoints based on NYSE stocks only
    filter(exchcd  ==  1) %>% # NYSE exchange
    group_by(Date) %>%
    summarize(var3.Q33 = quantile(!!var3, probs = (1/3), na.rm = TRUE),
              var3.Q66 = quantile(!!var3, probs = (2/3), na.rm = TRUE),
              snt.Q33 = quantile(!!snt, probs = (1/3), na.rm = TRUE),
              snt.Q66 = quantile(!!snt, probs = (2/3), na.rm = TRUE),
              size.Q50 = quantile(!!size, probs = .5, na.rm = TRUE),
              value.Q50 = quantile(!!value, probs = .5, na.rm = TRUE))
  
  # calculate size and var portfolio returns
  main.rank <- main.cln %>%
    left_join(Bkpts.NYSE, by = "Date") %>%
    mutate(Date = yearweek(Date),
           Size = ifelse(!!size < size.Q50, "S","B"), #small big
           Value = ifelse(!!value < value.Q50, "L","H"), #low high
           Var3 = ifelse(!!var3 < var3.Q33, "P1", ifelse(!!var3 < var3.Q66, "P2", "P3")), 
           SNT = ifelse(!!snt < snt.Q33, "N", ifelse(!!snt < snt.Q66, "M", "P"))) %>% #positive mid negative sentiment
    group_by(PERMNO) %>%
    mutate_at(vars(Size, Value, Var3, SNT), list(~ na.locf(., na.rm = FALSE))) %>%
    mutate(Port = paste(Size, Value, Var3, SNT, sep = ".")) %>%
    select(Date, PERMNO, Port)
  
  df <- main %>%
    select(Date, PERMNO, port.weight, retadj.1mn) %>%
    left_join(main.rank, by = c("Date", "PERMNO"), all.x = TRUE) %>% 
    mutate(Date = yearweek(Date)) %>% 
    group_by(PERMNO) %>%    
    mutate_at(vars(Port), list(~ na.locf(., na.rm = FALSE))) 
  
  Ret <- df %>% 
    group_by(Date, Port) %>%
    summarize(ret.port = weighted.mean(coalesce(retadj.1mn,0), coalesce(port.weight,0) , na.rm = TRUE)) %>% # calc value-weighted returns
    spread(Port, ret.port) %>% # transpose portfolios expressed as rows into seperate columns
    ungroup %>%
    join(rf, by = "Date") %>% 
    mutate(Date = yearweek(Date)) %>% 
    mutate_at(vars(-Date), list(~ (. - RF))) %>%
    select(-RF)
  
  return(Ret)
}
 
# copied from above
construct_portfolio <- function(df){
  # browser()
  output <- df %>% 
    ungroup %>%
    select(colnames(df[!grepl("NA", colnames(df))]))
}

construct_2x2x3x3_table <- function(df){
  # browser()
  rounder <- 10000 # 100 for %, 10000 for bps
  # small Size, small B/M
  output.S.L <- df %>% 
    ungroup %>%
    select(colnames(df[!grepl("NA", colnames(df)) & grepl("S.L", colnames(df))])) %>%
    as.data.frame %>%
    colMeans(na.rm = TRUE) %>%
    matrix(.,nrow = 3, ncol = 3, byrow = TRUE)
  rownames(output.S.L) <- c("Low", "Medium", "High")
  colnames(output.S.L) <- c("Mid", "Negative", "Positive")
  output.S.L <- output.S.L * rounder %>% round(2)
  output.S.L <- output.S.L[,c("Negative", "Mid", "Positive")]
  
  # small Size, high B/M
  output.S.H <- df %>% 
    ungroup %>%
    select(colnames(df[!grepl("NA", colnames(df)) & grepl("S.H.", colnames(df))])) %>%
    as.data.frame %>%
    colMeans(na.rm = TRUE) %>%
    matrix(.,nrow = 3, ncol = 3, byrow = TRUE)
  rownames(output.S.H) <- c("Low", "Medium", "High")
  colnames(output.S.H) <- c("Mid", "Negative", "Positive")
  output.S.H <- output.S.H * rounder %>% round(2) 
  output.S.H <- output.S.H[,c("Negative", "Mid", "Positive")]
  
  # large Size, small B/M
  output.B.L <- df %>% 
    ungroup %>%
    select(colnames(df[!grepl("NA", colnames(df)) & grepl("B.L.", colnames(df))])) %>%
    as.data.frame %>%
    colMeans(na.rm = TRUE) %>%
    matrix(.,nrow = 3, ncol = 3, byrow = TRUE)
  rownames(output.B.L) <- c("Low", "Medium", "High")
  colnames(output.B.L) <- c("Mid", "Negative", "Positive")
  output.B.L <- output.B.L * rounder %>% round(2) 
  output.B.L <- output.B.L[,c("Negative", "Mid", "Positive")]
  
  # large Size, high B/M
  output.B.H <- df %>% 
    ungroup %>%
    select(colnames(df[!grepl("NA", colnames(df)) & grepl("B.H.", colnames(df))])) %>%
    as.data.frame %>%
    colMeans(na.rm = TRUE) %>%
    matrix(.,nrow = 3, ncol = 3, byrow = TRUE)
  rownames(output.B.H) <- c("Low", "Medium", "High")
  colnames(output.B.H) <- c("Mid", "Negative", "Positive")
  output.B.H <- output.B.H * rounder %>% round(2) 
  output.B.H <- output.B.H[,c("Negative", "Mid", "Positive")]
  
  return(cbind(output.S.L, NA, output.S.H, NA, output.B.L, NA, output.B.H))
}

temp1 <- Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.OpIB), quo(snt.glo.diff), rf)
temp2 <- Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.AstChg), quo(snt.glo.diff), rf)
temp3 <- Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.ret.12t2), quo(snt.glo.diff), rf)
temp4 <- Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.OpIB), quo(snt.diff), rf)
temp5 <- Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.AstChg), quo(snt.diff), rf)
temp6 <- Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.ret.12t2), quo(snt.diff), rf)
temp7 <- Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.OpIB), quo(snt), rf)
temp8 <- Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.AstChg), quo(snt), rf)
temp9 <- Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.ret.12t2), quo(snt), rf)
# Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.OpIB), quo(lag.AstChg), rf),
# Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.OpIB), quo(lag.ret.12t2), rf),
# Form_2x2x3x3_portfolios_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.AstChg), quo(lag.ret.12t2), rf),
list.2x2x3x3.raw.d <- list(temp1, temp2, temp3, temp4, temp5, temp6, temp7, temp8, temp9)
save(list.2x2x3x3.raw.d, file = paste(filepath, "list.2x2x3x3.raw.d.RData", sep = ""))
rm(temp1, temp2, temp3, temp4, temp5, temp6, temp7, temp8, temp9)

list.2x2x3x3.pf.d <- list.2x2x3x3.raw.d %>% lapply(construct_portfolio)
save(list.2x2x3x3.pf.d, file = paste(filepath, "list.2x2x3x3.pf.d.RData", sep = ""))

table_2x2x3x3.d <- list.2x2x3x3.raw.d %>% lapply(construct_2x2x3x3_table)
save(table_2x2x3x3.d, file = paste(filepath, "table_2x2x3x3.d.RData", sep = ""))

attr(table_2x2x3x3.d, "subheadings") <- c("Panel A: Size-B/M-OP-SNT Portfolios",
                                          "Panel B: Size-B/M-Inv-SNT Portfolios",
                                          "Panel C: Size-B/M-Mom-SNT Portfolios",
                                          
                                          "Reference Previous Day: Size-B/M-OP-SNT Portfolios",
                                          "Reference Previous Day: Size-B/M-Inv-SNT Portfolios",
                                          "Reference Previous Day: Size-B/M-Mom-SNT Portfolios",
                                          
                                          "Reference Sentiement Level: Size-B/M-OP-SNT Portfolios",
                                          "Reference Sentiement Level: Size-B/M-Inv-SNT Portfolios",
                                          "Reference Sentiement Level: Size-B/M-Mom-SNT Portfolios")

table_2x2x3x3.xtL.d <- table_2x2x3x3.d %>% 
  xtableList(colNames = FALSE,
             caption = "2x2x3x3 Portfolios",
             include.rownames = TRUE,
             label = "table_2x2x3x3.d",
             digits = 2,
             align = "lrrrcrrrcrrrcrrr")

save(table_2x2x3x3.xtL.d, file = paste(filepath, "table_2x2x3x3.xtL.d.RData", sep = ""))

print.xtableList(table_2x2x3x3.xtL.d,
                 tabular.environment = "tabularx",
                 file = "table_2x2x3x3.tex",
                 size = "tiny",
                 caption.placement = "top",
                 width = "\\textwidth",
                 include.rownames = TRUE)

rm(list.2x2x3x3.raw.d, list.2x2x3x3.pf.d, table_2x2x3x3.d, table_2x2x3x3.xtL.d)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Form Factors ####

# Form Factors using 2x3 sorts ####
load(paste(filepath, "rf.RData", sep = ""))
load(paste(filepath, "data.snt.RData", sep = ""))

Form_2x3_Factors_new <- function(main, size, var) { # streamlined version
  # forms 2x3 (size x specificed-characteristc) and forms the 6 portfolios
  # variable broken by 30-70 percentiles, size broken up at 50 percentile (breakpoints uses NYSE data only)
  # requires Date and exchcd
  # outputs portfolio returns for each period,
  # browser()
  main.cln <- main %>%
    select(Date, PERMNO, exchcd, !!size, !!var)
  
  Bkpts.NYSE <- main.cln %>% # create size and var breakpoints based on NYSE stocks only
    filter(exchcd  ==  1) %>% # NYSE exchange
    group_by(Date) %>%
    summarize(var.P70 = quantile(!!var, probs = .7, na.rm = TRUE),
              var.P30 = quantile(!!var, probs = .3, na.rm = TRUE),
              size.Med = quantile(!!size, probs = .5, na.rm = TRUE))
  
  # calculate size and var portfolio returns
  main.rank <- main.cln %>%
    left_join(Bkpts.NYSE, by = "Date") %>%
    mutate(Size = ifelse(!!size < size.Med, "Small", "Big"),
           Var = ifelse(!!var < var.P30, "Low", ifelse(!!var > var.P70, "High", "Neutral"))) %>%
    group_by(PERMNO) %>% #* NEW
    mutate_at(vars(Size, Var), list(~ na.locf(., na.rm = FALSE))) %>%
    mutate(Port = paste(Size, Var, sep = "."))
  
  df <- main %>%
    select(Date, PERMNO, port.weight, retadj.1mn) %>%
    left_join(main.rank, by = c("Date", "PERMNO")) %>%
    group_by(PERMNO) %>%    
    mutate_at(vars(Port), list(~ na.locf(., na.rm = FALSE)))
  
  Ret <- df %>% # name 2 x 3 size-var portfolios
    group_by(Date, Port) %>%
    summarize(ret.port = weighted.mean(retadj.1mn, port.weight, na.rm = TRUE)) %>% # calc value-weighted returns
    spread(Port, ret.port) %>% # transpose portfolios expressed as rows into seperate columns
    mutate(Small = mean(c(Small.High, Small.Neutral, Small.Low), na.rm = T),
           Big = mean(c(Big.High, Big.Neutral, Big.Low), na.rm = T),
           SMB = coalesce(Small, 0) - coalesce(Big, 0),
           High = mean(c(Small.High, Big.High), na.rm = T),
           Low = mean(c(Small.Low, Big.Low), na.rm = T),
           Neutral = mean(c(Small.Neutral, Big.Neutral), na.rm = T),
           HML = coalesce(High, 0) - coalesce(Low, 0))
  
  return(Ret)
}

Form_FF6Ports_snt <- function(df, snt, rf) {
  # form FF6 factors from data (SMB, HML, RMW, CMA, UMD)
  #browser()
  output <- df %>%
    group_by(Date) %>%
    mutate(retadj.1mn = coalesce(retadj.1mn, 0),
           port.weight = coalesce(port.weight,0)) %>%
    summarize(MyMkt = weighted.mean(retadj.1mn, w = port.weight , na.rm = TRUE)) %>%
    left_join(rf, by = "Date") %>%
    mutate(MktRf = MyMkt - RF) %>%
    left_join(Form_2x3_Factors_new(df, quo(lag.ME.Jun), quo(lag.BM.FF)),
          by = "Date") %>% # SMB, HML
    select(Date:MktRf, MySMB = SMB, MySMBS = Small, MySMBB = Big, MyHML = HML, MyHMLH = High, MyHMLL = Low) %>%
    left_join(Form_2x3_Factors_new(df, quo(lag.ME.Jun), quo(lag.OpIB)),
          by = "Date") %>% # RMW
    select(Date:MyHMLL, MyRMW = HML, MyRMWR = High, MyRMWW = Low) %>%
    left_join(Form_2x3_Factors_new(df, quo(lag.ME.Jun), quo(lag.AstChg)),
          by = "Date") %>% # CMA
    select(Date:MyRMWW, MyCMA = HML, MyCMAC = Low, MyCMAA = High) %>%
    mutate(MyCMA = -MyCMA) %>%
    left_join(Form_2x3_Factors_new(df, quo(lag.ME.Jun), quo(lag.ret.12t2)), 
          by = "Date") %>% # UMD
    select(Date:MyCMAA, MyUMD = HML, MyUMDU = High, MyUMDD = Low) %>%
    left_join(Form_2x3_Factors_new(df, quo(lag.ME.Jun), quo(!!snt)), 
          by = "Date") %>% # PMN
    select(Date:MyUMDD, MyPMN = HML, MyPMNP = High, MyPMNN = Low, MyPMNNeu = Neutral)
  return(output)
}

sorts.2x3 <- Form_FF6Ports_snt(data.snt, quo(snt.glo.diff), rf)
colnames(sorts.2x3) <- sub("My", "", colnames(sorts.2x3))
save(sorts.2x3, file = paste(filepath, "sorts.2x3.RData", sep = ""))

sorts.2x3.diff <- Form_FF6Ports_snt(data.snt, quo(snt.diff), rf)
colnames(sorts.2x3.diff) <- sub("My", "", colnames(sorts.2x3.diff))
save(sorts.2x3.diff, file = paste(filepath, "sorts.2x3.diff.RData", sep = ""))

sorts.2x3.lvl <- Form_FF6Ports_snt(data.snt, quo(snt), rf)
colnames(sorts.2x3.lvl) <- sub("My", "", colnames(sorts.2x3.lvl))
save(sorts.2x3.lvl, file = paste(filepath, "sorts.2x3.lvl.RData", sep = ""))

# Form Factors using 2x2x2x2x2x2 portfolios ####
load(paste(filepath, "data.snt.RData", sep = ""))
load(paste(filepath, "rf.RData", sep = ""))

Form_2x2x2x2x2x2_Factors_new <- function(main, size, BM, OP, Inv, Mom, Snt) { # streamlined version
  # forms sorted portfolios 2x2x2x2x2x2(size x specificed-characteristc1 x specificed-characteristc2 etc.) and forms 64 portfolios
  # variable broken by NYSE medians data only)
  # requires Date and exchcd
  # outputs portfolio *excess* returns for each period,
   # browser()
  main.cln <- main %>%
    select(Date, PERMNO, exchcd, !!size, !!BM, !!OP, !!Inv, !!Mom, !!Snt)
  
  Bkpts.NYSE <- main.cln %>% # create size and var breakpoints based on NYSE stocks only
    filter(exchcd  ==  1) %>% # NYSE exchange
    group_by(Date) %>%
    summarize(size.P50 = quantile(!!size, probs = .50, na.rm = TRUE),
              BM.P50 = quantile(!!BM, probs = .50, na.rm = TRUE),
              OP.P50 = quantile(!!OP, probs = .50, na.rm = TRUE),
              Inv.P50 = quantile(!!Inv, probs = .50, na.rm = TRUE),
              Mom.P50 = quantile(!!Mom, probs = .50, na.rm = TRUE),
              Snt.P50 = quantile(!!Snt, probs = .50, na.rm = TRUE))
  
  # calculate size and var portfolio returns
  main.rank <- main.cln %>%
    left_join(Bkpts.NYSE, by = "Date") %>%
    mutate(Size = ifelse(!!size < size.P50, "S","B"), #small big
           BM = ifelse(!!BM < BM.P50, "L","H"), 
           OP = ifelse(!!OP < OP.P50, "W","R"), 
           Inv = ifelse(!!Inv < Inv.P50, "C","G"), # G used instead of A to avoid confusion with NA later
           Mom = ifelse(!!Mom < Mom.P50, "D","U"),
           Snt = ifelse(!!Snt < Snt.P50, "M","P")) %>%    # M used instead of N to avoid confusion with NA later
    group_by(PERMNO) %>%
    mutate_at(vars(Size, BM, OP, Inv, Mom, Snt), list(~ na.locf(., na.rm = FALSE))) %>%
    mutate(Port = paste(Size, BM, OP, Inv, Mom, Snt, sep = "."))
  
  df <- main %>%
    select(Date, PERMNO, port.weight, retadj.1mn) %>%
    left_join(main.rank, by = c("Date", "PERMNO")) %>%
    group_by(PERMNO) %>%    
    mutate_at(vars(Port), list(~ na.locf(., na.rm = FALSE)))
  
  pfs <- df %>% # name 2x2x2x2x2x2 portfolios
    group_by(Date, Port) %>%
    # mutate(retadj.1mn = coalesce(retadj.1mn,0),
    #        port.weight = coalesce(port.weight,0))  %>%
    summarize(ret.port = weighted.mean(retadj.1mn, w = port.weight, na.rm = TRUE)) %>%
    spread(Port, ret.port)  # transpose portfolios expressed as rows into seperate columns
  
  Small <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("S", colnames(.))])) %>% #small
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Small = rowMeans(., na.rm = TRUE)) %>%
    select(Small)
  
  Big <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("B", colnames(.))])) %>% # big
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Big = rowMeans(., na.rm = TRUE)) %>%
    select(Big)
  
  High <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("H", colnames(.))])) %>% # high
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(High = rowMeans(., na.rm = TRUE)) %>%
    select(High)
  
  Low <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("L", colnames(.))])) %>% # low
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Low = rowMeans(., na.rm = TRUE)) %>%
    select(Low)
  
  Robust <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("R", colnames(.))])) %>% # robust
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Robust = rowMeans(., na.rm = TRUE)) %>%
    select(Robust)
  
  Weak <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("W", colnames(.))])) %>% # weak
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Weak = rowMeans(., na.rm = TRUE)) %>%
    select(Weak)
  
  Conservative <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("C", colnames(.))])) %>% # conservative
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Conservative = rowMeans(., na.rm = TRUE)) %>%
    select(Conservative)
  
  Aggressive <- pfs %>% 
    as.data.frame() %>%
    select(colnames(.[grepl("G", colnames(.))])) %>% # aggressive
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Aggressive = rowMeans(., na.rm = TRUE)) %>%
    select(Aggressive)
  
  Up <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("U", colnames(.))])) %>% # up
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Up = rowMeans(., na.rm = TRUE)) %>%
    select(Up)
  
  Down <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("D", colnames(.))]), -Date) %>% # down
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Down = rowMeans(., na.rm = TRUE)) %>%
    select(Down)
  
  Positive <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("P", colnames(.))])) %>% # positive
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Positive = rowMeans(., na.rm = TRUE)) %>%
    select(Positive)
  
  Negative <- pfs %>%
    as.data.frame() %>%
    select(colnames(.[grepl("M", colnames(.))])) %>% # negative
    select(-c(colnames(.[grepl("NA", colnames(.))]))) %>% #small
    mutate(Negative = rowMeans(., na.rm = TRUE)) %>%
    select(Negative)
  
  Ret <- pfs %>% 
    select(Date) %>% 
    as.data.frame %>%
    cbind(Small, Big, High, Low, Robust, Weak, Conservative, Aggressive, Up, Down, Positive, Negative) %>% 
    mutate(SMB = coalesce(Small,0) - coalesce(Big,0),
           HML = coalesce(High,0) - coalesce(Low,0),
           RMW = coalesce(Robust,0) - coalesce(Weak,0),
           CMA = coalesce(Conservative,0) - coalesce(Aggressive,0),
           UMD = coalesce(Up,0) - coalesce(Down,0),
           PMN = coalesce(Positive,0) - coalesce(Negative,0))
  
  return(Ret)
  # when creating the factors we do not deduct the risk free rate
}

Form_FF6Ports_snt <- function(df, factors, rf) {
  # form FF6 factors from data (SMB, HML, RMW, CMA, UMD, PMN)
   # browser()  
  df <- df %>%
    group_by(Date) %>%
    mutate(retadj.1mn = coalesce(retadj.1mn,0),
           port.weight = coalesce(port.weight,0))  %>%
    summarize(Mkt = weighted.mean(retadj.1mn, w = port.weight, na.rm = TRUE)) %>%
    left_join(rf, by = "Date") %>%
    mutate(Mkt = coalesce(Mkt,0),
           RF = coalesce(RF,0),
           MktRf = Mkt - RF)
  
  output <- factors %>%
    group_by(Date) %>%
    select(Date, SMB, SMBS = Small, SMBB = Big, HML, HMLH = High, HMLL = Low, RMW, RMWR = Robust, RMWW = Weak, CMA, 
           CMAC = Conservative, CMAA = Aggressive, UMD, UMDU = Up, UMDD = Down, PMN, PMNP = Positive, PMNN = Negative) %>%
    merge(df, by = "Date", all = TRUE)

  return(output)
}

Form_FF6Ports_snt <- function(df, factors, rf) {
  # form FF6 factors from data (SMB, HML, RMW, CMA, UMD, PMN)
  # browser()  
  df <- df %>%
    group_by(Date) %>%
    mutate(retadj.1mn = coalesce(retadj.1mn,0),
           port.weight = coalesce(port.weight,0))  %>%
    summarize(Mkt = weighted.mean(retadj.1mn, w = port.weight, na.rm = TRUE)) %>%
    left_join(rf, by = "Date") %>%
    mutate(Mkt = coalesce(Mkt,0),
           RF = coalesce(RF,0),
           MktRf = Mkt - RF)
  
  output <- factors %>%
    group_by(Date) %>%
    select(Date, SMB, SMBS = Small, SMBB = Big, HML, HMLH = High, HMLL = Low, RMW, RMWR = Robust, RMWW = Weak, CMA, 
           CMAC = Conservative, CMAA = Aggressive, UMD, UMDU = Up, UMDD = Down, PMN, PMNP = Positive, PMNN = Negative) %>%
    full_join(df, by = "Date") %>%
    ungroup
  
  return(output)
}

factors <- Form_2x2x2x2x2x2_Factors_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.OpIB), quo(lag.AstChg),
                                                         quo(lag.ret.12t2), quo(snt.glo.diff)) 

factors.diff <- Form_2x2x2x2x2x2_Factors_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.OpIB), quo(lag.AstChg),
                                        quo(lag.ret.12t2), quo(snt.diff)) 

factors.lvl <- Form_2x2x2x2x2x2_Factors_new(data.snt, quo(lag.ME.Jun), quo(lag.BM.FF), quo(lag.OpIB), quo(lag.AstChg),
                                        quo(lag.ret.12t2), quo(snt)) 

snt.sorts.2x2x2x2x2x2 <- Form_FF6Ports_snt(data.snt, factors, rf)
save(snt.sorts.2x2x2x2x2x2, file = paste(filepath, "snt.sorts.2x2x2x2x2x2.RData", sep = ""))

snt.sorts.2x2x2x2x2x2.diff <- Form_FF6Ports_snt(data.snt, factors.diff, rf)
save(snt.sorts.2x2x2x2x2x2.diff, file = paste(filepath, "snt.sorts.2x2x2x2x2x2.diff.RData", sep = ""))

snt.sorts.2x2x2x2x2x2.lvl <- Form_FF6Ports_snt(data.snt, factors.lvl, rf)
save(snt.sorts.2x2x2x2x2x2.lvl, file = paste(filepath, "snt.sorts.2x2x2x2x2x2.lvl.RData", sep = ""))

rm(factors, factors.diff, factors.lvl)

# Define specification for analysis and regressions ####
load(paste(filepath, "dt.FF6.RData", sep = ""))
load(paste(filepath, "snt.sorts.2x2x2x2x2x2.RData", sep = ""))
load(paste(filepath, "sorts.2x3.RData", sep = ""))
load(paste(filepath, "snt.sorts.2x2x2x2x2x2.diff.RData", sep = ""))
load(paste(filepath, "sorts.2x3.diff.RData", sep = ""))
load(paste(filepath, "snt.sorts.2x2x2x2x2x2.lvl.RData", sep = ""))
load(paste(filepath, "sorts.2x3.lvl.RData", sep = ""))

sort.selector <- 1 #1: glo.diff 2: diff 3: level
factor.selector <- 1  #1: 2x3 sorts, 2: 2x2x2x2x2x2 sorts

if (sort.selector  ==  1 && factor.selector == 1) {
  sorts <- sorts.2x3
} else if (sort.selector  ==  2 && factor.selector == 1) {
  sorts <- sorts.2x3.diff
} else if (sort.selector  ==  3 && factor.selector == 1) {
  sorts <- sorts.2x3.lvl
} else if (sort.selector  ==  1 && factor.selector == 2) {
  sorts <- snt.sorts.2x2x2x2x2x2
} else if (sort.selector  ==  2 && factor.selector == 2) {
  sorts <- snt.sorts.2x2x2x2x2x2.diff
} else if (sort.selector  ==  3 && factor.selector == 2) {
  sorts <- snt.sorts.2x2x2x2x2x2.lvl
}

save(sorts, file = paste(filepath, "sorts.RData", sep = ""))
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Preparation ####

load(paste(filepath, "sorts.RData", sep = ""))
load(paste(filepath, "list.5x5.pf.d.RData", sep = ""))

y <- "mimicking portfolios" #indicator for WRDS

run_model <- function(df, factors){
  # browser()
  model_formula <- as.formula(paste("Ret ~ ", paste(colnames(select(factors, -Date)), collapse = " + "), sep = ""))
  print(model_formula)
  
  model <- df %>% 
    gather(key = "port", value = "Ret", -Date) %>%
    plyr::join(factors, by = "Date") %>%
    group_by(port) %>%
    do(fit.model = lm(model_formula, data = .))
  
  model.coef <- tidy(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.pred <- augment(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.stats <- glance(model, fit.model) # get the coefficients by group in a tidy data_frame
  
  return(list(model,
              model.coef,
              model.pred,
              model.stats))
}

# Model performance on factor 25 mimicking portfolios ####

# Define models
ff3.factors <- sorts %>%  select(Date, MktRf, SMB, HML)
ff3.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, PMN)
ff5.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA)
ff5.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, PMN)
ch4.factors <- sorts %>% select(Date, MktRf, SMB, HML, UMD)
ch4.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, UMD, PMN)
full.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, UMD)
full.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, UMD, PMN)

# Run models
list.ff3 <- list.5x5.pf.d %>% lapply(run_model, ff3.factors)
list.ff3.snt <- list.5x5.pf.d %>% lapply(run_model, ff3.snt.factors)
list.ff5 <- list.5x5.pf.d %>% lapply(run_model, ff5.factors)
list.ff5.snt <- list.5x5.pf.d %>% lapply(run_model, ff5.snt.factors)
list.ch4 <- list.5x5.pf.d %>% lapply(run_model, ch4.factors)
list.ch4.snt <- list.5x5.pf.d %>% lapply(run_model, ch4.snt.factors)
list.full <- list.5x5.pf.d %>% lapply(run_model, full.factors)
list.full.snt <- list.5x5.pf.d %>% lapply(run_model, full.snt.factors)

# save models
# save(list.ff3, file = paste(filepath, "sorts.list.ff3.RData", sep = ""))
# save(list.ff3.snt, file = paste(filepath, "sorts.list.ff3.snt.RData", sep = ""))
# save(list.ff5, file = paste(filepath, "sorts.list.ff5.RData", sep = ""))
# save(list.ff5.snt, file = paste(filepath, "sorts.list.ff5.snt.RData", sep = ""))
# save(list.ch4, file = paste(filepath, "sorts.list.ch4.RData", sep = ""))
# save(list.ch4.snt, file = paste(filepath, "sorts.list.ch4.snt.RData", sep = ""))
# save(list.full, file = paste(filepath, "sorts.list.full.RData", sep = ""))
# save(list.full.snt, file = paste(filepath, "sorts.list.full.snt.RData", sep = ""))

# Model performance on factor 25 mimicking portfolios - sentiment split ####

sorts %<>% 
  na.omit() %>% 
  mutate(orthoPMN = lm(PMN ~ MktRf + SMB + HML + RMW + CMA + UMD, data = .)$residuals,
         orthoPMNP = lm(PMNP ~ MktRf + SMB + HML + RMW + CMA + UMD, data = .)$residuals,
         orthoPMNN = lm(PMNN ~ MktRf + SMB + HML + RMW + CMA + UMD + orthoPMNP, data = .)$residuals)

# Define models
ff3.factors <- sorts %>%  select(Date, MktRf, SMB, HML)
ff3.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, orthoPMNP, orthoPMNN)
ff5.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA)
ff5.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, orthoPMNP, orthoPMNN)
ch4.factors <- sorts %>% select(Date, MktRf, SMB, HML, UMD)
ch4.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, UMD, orthoPMNP, orthoPMNN)
full.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, UMD)
full.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, UMD, orthoPMNP, orthoPMNN)

# stats.calc <- function(df){
#   return(df[[3]] %>%
#            group_by(port) %>% 
#            mutate(Vol = rollapply( .resid ^ 2, 4, "mean", na.rm = T, fill = NA) ^ 0.5,  # idiosyncratic volatility
#                   Skew = (rollapply( .resid ^ 3, 4, "mean", na.rm = T, fill = NA)/Vol ^ 3) / 4,  # idiosyncratic skewnewss
#                   Kurt = (rollapply( .resid ^ 4, 4, "mean", na.rm = T, fill = NA)/Vol ^ 4) / 4, # idiosyncratic kurtosis
#                   dVol = Vol / lag(Vol),
#                   dSkew = Skew / lag(Skew),
#                   dKurt = Kurt / lag(Kurt)))
# }
# 
# test <- list.ff3 %>% lapply(stats.calc)
# test2 <- list.5x5.pf.d %>%
#   join(test, by = "port")

# Run models
list.ff3 <- list.5x5.pf.d %>% lapply(run_model, ff3.factors)
list.ff3.snt <- list.5x5.pf.d %>% lapply(run_model, ff3.snt.factors)
list.ff5 <- list.5x5.pf.d %>% lapply(run_model, ff5.factors)
list.ff5.snt <- list.5x5.pf.d %>% lapply(run_model, ff5.snt.factors)
list.ch4 <- list.5x5.pf.d %>% lapply(run_model, ch4.factors)
list.ch4.snt <- list.5x5.pf.d %>% lapply(run_model, ch4.snt.factors)
list.full <- list.5x5.pf.d %>% lapply(run_model, full.factors)
list.full.snt <- list.5x5.pf.d %>% lapply(run_model, full.snt.factors)

as.data.frame(list.ff3.snt[[1]][[2]] %>% 
                filter(term %in% c("orthoPMNP", "orthoPMNN")) %>% 
                filter(p.value < 0.1))

# save models
# save(list.ff3, file = paste(filepath, "sorts.list.ff3.RData", sep = ""))
# save(list.ff3.snt, file = paste(filepath, "sorts.list.ff3.snt.RData", sep = ""))
# save(list.ff5, file = paste(filepath, "sorts.list.ff5.RData", sep = ""))
# save(list.ff5.snt, file = paste(filepath, "sorts.list.ff5.snt.RData", sep = ""))
# save(list.ch4, file = paste(filepath, "sorts.list.ch4.RData", sep = ""))
# save(list.ch4.snt, file = paste(filepath, "sorts.list.ch4.snt.RData", sep = ""))
# save(list.full, file = paste(filepath, "sorts.list.full.RData", sep = ""))
# save(list.full.snt, file = paste(filepath, "sorts.list.full.snt.RData", sep = ""))

# Model performance on factor 25 mimicking portfolios incl higher moments of PMN ####

load(paste(filepath, "sorts.RData", sep = ""))
load(paste(filepath, "list.5x5.pf.d.RData", sep = ""))

y <- "mimicking portfolios" #indicator for WRDS

run_model <- function(df, factors){
  # browser()
  model_formula <- as.formula(paste("Ret ~ ", paste(colnames(select(factors, -Date)), collapse = " + "), sep = ""))
  print(model_formula)
  
  model <- df %>% 
    gather(key = "port", value = "Ret", -Date) %>%
    join(factors, by = "Date") %>%
    group_by(port) %>%
    do(fit.model = lm(model_formula, data = .))
  
  model.coef <- tidy(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.pred <- augment(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.stats <- glance(model, fit.model) # get the coefficients by group in a tidy data_frame
  
  return(list(model,
              model.coef,
              model.pred,
              model.stats))
}

# Define models
ff3.factors <- sorts %>%  select(Date, MktRf, SMB, HML)
ff3.snt.factors <- sorts %>% 
  mutate(squaredSNT = PMN ^ 2,
         thirdSNT = PMN ^ 3,
         fourthSNT = PMN ^ 4) %>% 
  select(Date, MktRf, SMB, HML, PMN, squaredSNT, thirdSNT, fourthSNT)
ff5.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA)
ff5.snt.factors <- sorts %>% 
  mutate(squaredSNT = PMN ^ 2,
         thirdSNT = PMN ^ 3,
         fourthSNT = PMN ^ 4) %>% 
  select(Date, MktRf, SMB, HML, RMW, CMA, PMN, squaredSNT, thirdSNT, fourthSNT)
ch4.factors <- sorts %>% select(Date, MktRf, SMB, HML, UMD)
ch4.snt.factors <- sorts %>% 
  mutate(squaredSNT = PMN ^ 2,
         thirdSNT = PMN ^ 3,
         fourthSNT = PMN ^ 4) %>% 
  select(Date, MktRf, SMB, HML, UMD, PMN, squaredSNT, thirdSNT, fourthSNT)
full.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, UMD)
full.snt.factors <- sorts %>% 
  mutate(squaredSNT = PMN ^ 2,
         thirdSNT = PMN ^ 3,
         fourthSNT = PMN ^ 4) %>% 
  select(Date, MktRf, SMB, HML, RMW, CMA, UMD, PMN, squaredSNT, thirdSNT, fourthSNT)

# Run models
list.ff3 <- list.5x5.pf.d %>% lapply(run_model, ff3.factors)
list.ff3.snt <- list.5x5.pf.d %>% lapply(run_model, ff3.snt.factors)
list.ff5 <- list.5x5.pf.d %>% lapply(run_model, ff5.factors)
list.ff5.snt <- list.5x5.pf.d %>% lapply(run_model, ff5.snt.factors)
list.ch4 <- list.5x5.pf.d %>% lapply(run_model, ch4.factors)
list.ch4.snt <- list.5x5.pf.d %>% lapply(run_model, ch4.snt.factors)
list.full <- list.5x5.pf.d %>% lapply(run_model, full.factors)
list.full.snt <- list.5x5.pf.d %>% lapply(run_model, full.snt.factors)

# save models
# save(list.ff3, file = paste(filepath, "sorts.list.ff3.RData", sep = ""))
# save(list.ff3.snt, file = paste(filepath, "sorts.list.ff3.snt.RData", sep = ""))
# save(list.ff5, file = paste(filepath, "sorts.list.ff5.RData", sep = ""))
# save(list.ff5.snt, file = paste(filepath, "sorts.list.ff5.snt.RData", sep = ""))
# save(list.ch4, file = paste(filepath, "sorts.list.ch4.RData", sep = ""))
# save(list.ch4.snt, file = paste(filepath, "sorts.list.ch4.snt.RData", sep = ""))
# save(list.full, file = paste(filepath, "sorts.list.full.RData", sep = ""))
# save(list.full.snt, file = paste(filepath, "sorts.list.full.snt.RData", sep = ""))

# Average absolute intercept ####
# the smaller the better

# aai.fct <- function(model){
#   #browser()
#   
#   intercepts <- model[[2]] %>% 
#     filter(term == "(Intercept)") %>% 
#     select(estimate) %>% 
#     ungroup() 
#   
#   # print(summary(intercepts))
#   
#   aai <- intercepts %>% 
#     summarize(aai = mean(abs(estimate))*10000)
#   
#   return(aai)
#   
#   # mean(abs(model[[2]]$estimate[which(model[[2]]$term == "(Intercept)")])) * 10000#10000 for bps, 100 for %
# }
# 
# aai.stats <- list(list.ff3 %>% lapply(aai.fct) %>% list2df() %>% select(X1),
#                   list.ff3.snt %>% lapply(aai.fct) %>% list2df() %>% select(X1),
#                   list.ff5 %>% lapply(aai.fct) %>% list2df() %>% select(X1),
#                   list.ff5.snt %>% lapply(aai.fct) %>% list2df() %>% select(X1),
#                   list.ch4 %>% lapply(aai.fct) %>% list2df() %>% select(X1),
#                   list.ch4.snt %>% lapply(aai.fct) %>% list2df() %>% select(X1),
#                   list.full %>% lapply(aai.fct) %>% list2df() %>% select(X1),
#                   list.full.snt %>% lapply(aai.fct) %>% list2df() %>% select(X1)
# )

aai.fct <- function(model){
  #browser()
  mean(abs(model[[2]]$estimate[which(model[[2]]$term == "(Intercept)")])) * 10000#10000 for bps, 100 for %
}

for (i in 1:length(list.ff3)) {
  aai.stats[[i]] <- rbind(
    list.ff3[[i]] %>% aai.fct,
    list.ff3.snt[[i]] %>% aai.fct,
    list.ff5[[i]] %>% aai.fct,
    list.ff5.snt[[i]] %>% aai.fct,
    list.ch4[[i]] %>% aai.fct,
    list.ch4.snt[[i]] %>% aai.fct,
    list.full[[i]] %>% aai.fct,
    list.full.snt[[i]] %>% aai.fct
  )
}

list.ff3[[1]][[2]] %>% filter(term == "(Intercept)")
list.ff3.snt[[1]][[2]] %>% filter(term == "(Intercept)")
list.ff3.snt[[1]][[2]] %>% filter(term %in% c("orthoPMNP", "orthoPMNN"))

print(aai.stats)
save(aai.stats, file = paste(filepath, "aai.stats.RData", sep = ""))

# # show significance of coefficients
# for (i in 1:length(list.ff3)) {
# print(list.ff3.snt[[i]][[2]] %>%
#   filter(term == "PMN" | term == "squaredSNT" | term == "thirdSNT" | term == "fourthSNT",
#   # filter(term == "PMN" | term == "PMNN" | term == "PMNP",
#          p.value < 0.05) %>% 
#   as.data.frame)
# }

# GRS test statistic for mimicking portfolios ####
# test cannot handle NA value
# if alpha = 0, thenony  the GRS statistic equals zero; the larger the alphas are in
# absolute value the greater the GRS statistic will be.

# Function for significance stars
sig.stars <- function(x) {
  if (x < 0.01) stars <- '***'
  else if (x < 0.05) stars <- '**'
  else if (x < 0.1) stars <- '*'
  else if (x > 0.1) stars <- ''
  return(stars)
}

GRS.fct <- function(df, factors){
  #browser()
  comb <- df %>%
    merge(factors, by = "Date") %>%
    na.omit 
  
  GRS.score <- GRS.test(comb[,2:ncol(df)], comb[,(ncol(df) + 1):ncol(comb)])$GRS.stat %>%
    round(4) %>%
    format(nsmall = 4)
  
  GRS.pValue <- GRS.test(comb[,2:ncol(df)], comb[,(ncol(df) + 1):ncol(comb)])$GRS.pval %>% sig.stars
  
  return(paste(GRS.score, GRS.pValue, sep = ""))
}

# the smaller the better

GRS.stats <- list()

for (i in 1:length(list.5x5.pf.d)) {
  GRS.stats[[i]] <- rbind(
    list.5x5.pf.d[[i]] %>% GRS.fct(ff3.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ff3.snt.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ff5.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ff5.snt.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ch4.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ch4.snt.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(full.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(full.snt.factors)
  )
}

print(GRS.stats)
save(GRS.stats, file = paste(filepath, "GRS.stats.RData", sep = ""))

# p Value of intercepts ####

aai.p.value.fct <- function(model){
  mean(abs(model[[2]]$p.value[which(model[[2]]$term == "(Intercept)")]))
}

aai.p.value.stats <- list()

for (i in 1:length(list.ff3)) {
  aai.p.value.stats[[i]] <- rbind(
    list.ff3[[i]] %>% aai.p.value.fct,
    list.ff3.snt[[i]] %>% aai.p.value.fct,
    list.ff5[[i]] %>% aai.p.value.fct,
    list.ff5.snt[[i]] %>% aai.p.value.fct,
    list.ch4[[i]] %>% aai.p.value.fct,
    list.ch4.snt[[i]] %>% aai.p.value.fct,
    list.full[[i]] %>% aai.p.value.fct,
    list.full.snt[[i]] %>% aai.p.value.fct
  )
}

print(aai.p.value.stats)
save(aai.p.value.stats, file = paste(filepath, "aai.p.value.stats.RData", sep = ""))


# Average absolute intercept over average absolute value of r ####
# the smaller the better
library(matrixStats) # for ColMedians

aai.fct.unscaled <- function(model){
  #browser()
  median(abs(model[[2]]$estimate[which(model[[2]]$term == "(Intercept)")]))
}

aaiOaar.fct <- function(df, model){
  #browser()
  Ri <- df %>% # time series average excess return of portfolio i
    select(-Date) %>%
    as.matrix %>%
    colMedians(na.rm = TRUE)
  Rbar <- median(Ri, na.rm = T) # cross-section average of Ri
  r <- Ri - Rbar
  aai <- model %>% aai.fct.unscaled
  aai / median(abs(r))
}

aaiOaar.stats <- list()

for (i in 1:length(list.ff3)) {
  aaiOaar.stats[[i]] <-
    rbind(
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ff3[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ff3.snt[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ff5[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ff5.snt[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ch4[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ch4.snt[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.full[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.full.snt[[i]])
    )
}

print(aaiOaar.stats)
save(aaiOaar.stats, file = paste(filepath, "aaiOaar.stats.RData", sep = ""))


# Average absolute squared intercept over average absolute squared value of r - NOT WORKING ####

aai.fct.unscaled2 <- function(model){
  #browser()
  median(model[[2]]$estimate[which(model[[2]]$term == "(Intercept)")] ^ 2 -
           model[[2]]$std.error[which(model[[2]]$term == "(Intercept)")] ^ 2) 
}

aaiOaar2.fct <- function(df, model){
  Ri <- df %>% # time series average excess return of portfolio i
    select(-Day) %>%
    as.matrix %>%
    colMedians(na.rm = TRUE)
  Rbar <- median(Ri ^ 2, na.rm = T) # cross-section average of Ri
  r <- (Ri - Rbar) ^ 2
  asqrd <- model %>% aai.fct.unscaled2
  median(asqrd) / median(r)
}

aaiOaar2.stats <- list(
  rbind(list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ff3[[1]]), # size_HML.pf
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ff3.snt[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ff5[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ff5.snt[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ch4[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ch4.snt[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.full[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.full.snt[[1]])),
  rbind(list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ff3[[2]]), # size_RMW.pf
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ff3.snt[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ff5[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ff5.snt[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ch4[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ch4.snt[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.full[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.full.snt[[2]])),
  rbind(list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ff3[[3]]), # size_CMA.pf
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ff3.snt[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ff5[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ff5.snt[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ch4[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ch4.snt[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.full[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.full.snt[[3]])),
  rbind(list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ff3[[4]]), # size_UMD.pf
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ff3.snt[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ff5[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ff5.snt[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ch4[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ch4.snt[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.full[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.full.snt[[4]])),
  rbind(list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ff3[[5]]), # size_PMN.pf
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ff3.snt[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ff5[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ff5.snt[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ch4[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ch4.snt[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.full[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.full.snt[[5]])))

save(aaiOaar2.stats, file = paste(filepath, "aaiOaar2.stats.RData", sep = ""))


# Average 1-R2 as a similar metric to the previous one ####

#the smaller the better

OnemR2.fct <- function(model){
  mean(abs(model[[4]]$adj.r.squared))
}

OnemR2.stats <- list()


for (i in 1:length(list.ff3)) {
  OnemR2.stats[[i]] <- rbind(
    list.ff3[[i]] %>% OnemR2.fct,
    list.ff3.snt[[i]] %>% OnemR2.fct,
    list.ff5[[i]] %>% OnemR2.fct,
    list.ff5.snt[[i]] %>% OnemR2.fct,
    list.ch4[[i]] %>% OnemR2.fct,
    list.ch4.snt[[i]] %>% OnemR2.fct,
    list.full[[i]] %>% OnemR2.fct,
    list.full.snt[[i]] %>% OnemR2.fct
  )
}
print(OnemR2.stats)
save(OnemR2.stats, file = paste(filepath, "OnemR2.stats.RData", sep = ""))
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Model performance on single stocks  ####

load(paste(filepath, "sorts.RData", sep = ""))
load(paste(filepath, "data.snt.RData", sep = ""))

stock.ret <- data.snt %>%
  select(Date, PERMNO, Ret = retadj.1mn) 

y <- "stocks" #indicator for WRDS

run_model_stocks <- function(df, factors){
  # browser()
  model_formula <- as.formula(paste("Ret ~ ", paste(colnames(select(factors, -Date)), collapse = " + "), sep = ""))
  print(model_formula)
  
  model <- df %>%
    join(factors, by = "Date") %>%
    filter(complete.cases(.)) %>%
    group_by(PERMNO) %>%
    do(fit.model = lm(model_formula, data = .))
  
  model.coef <- tidy(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.pred <- augment(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.stats <- glance(model, fit.model) # get the coefficients by group in a tidy data_frame
  
  return(list(model,
              model.coef,
              model.pred,
              model.stats))
}

# Define models
ff3.factors <- sorts %>%  select(Date, MktRf, SMB, HML)
ff3.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, PMN) 
ff5.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA)
ff5.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, PMN)
ch4.factors <- sorts %>% select(Date, MktRf, SMB, HML, UMD)
ch4.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, UMD, PMN)
full.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, UMD)
full.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, UMD, PMN)

# Run models
list.ff3 <- stock.ret %>% run_model_stocks(ff3.factors)
list.ff3.snt <- stock.ret %>% run_model_stocks(ff3.snt.factors)
list.ff5 <- stock.ret %>% run_model_stocks(ff5.factors)
list.ff5.snt <- stock.ret %>% run_model_stocks(ff5.snt.factors)
list.ch4 <- stock.ret %>% run_model_stocks(ch4.factors)
list.ch4.snt <- stock.ret %>% run_model_stocks(ch4.snt.factors)
list.full <- stock.ret %>% run_model_stocks(full.factors)
list.full.snt <- stock.ret %>% run_model_stocks(full.snt.factors)

# Save models
# save(list.ff3, file = paste(filepath, "list.ff3.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ff3.snt, file = paste(filepath, "list.ff3.snt.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ff5, file = paste(filepath, "list.ff5.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ff5.snt, file = paste(filepath, "list.ff5.snt.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ch4, file = paste(filepath, "list.ch4.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ch4.snt, file = paste(filepath, "list.ch4.snt.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.full, file = paste(filepath, "list.full.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.full.snt, file = paste(filepath, "list.full.snt.stocks.sorts.glo.diff.RData", sep = ""))

# load models if needed
# load(file = paste(filepath, "list.ff3.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ff3.snt.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ff5.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ff5.snt.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ch4.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ch4.snt.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.full.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.full.snt.stocks.RData", sep = ""))

# Average absolute intercept ####
# the smaller the better

aai.fct <- function(model){
  #browser()
  mean(abs(model[[2]]$estimate[which(model[[2]]$term == "(Intercept)")])) * 10000#10000 for bps, 100 for %
}

aai.stats <- list()

for (i in 1:1) {
  aai.stats[[i]] <- rbind(
    list.ff3 %>% aai.fct,
    list.ff3.snt %>% aai.fct,
    list.ff5 %>% aai.fct,
    list.ff5.snt %>% aai.fct,
    list.ch4 %>% aai.fct,
    list.ch4.snt %>% aai.fct,
    list.full %>% aai.fct,
    list.full.snt %>% aai.fct
  )
}

print(aai.stats)
save(aai.stats, file = paste(filepath, "aai.stats.RData", sep = ""))

# show significance of coefficients
# print(list.ff3.snt[[2]] %>%
#           filter(term == "PMN",
#                  p.value < 0.01) %>% 
#           as.data.frame)


# GRS test statistic for mimicking portfolios ####
# test cannot handle NA value
# if alpha = 0, thenony  the GRS statistic equals zero; the larger the alphas are in
# absolute value the greater the GRS statistic will be.

# Function for significance stars
sig.stars <- function(x) {
  if (x < 0.01) stars <- '***'
  else if (x < 0.05) stars <- '**'
  else if (x < 0.1) stars <- '*'
  else if (x > 0.1) stars <- ''
  return(stars)
}

GRS.fct <- function(df, factors){
  #browser()
  comb <- df %>%
    merge(factors, by = "Day") %>%
    na.omit 
  
  GRS.score <- GRS.test(comb[,2:ncol(df)], comb[,(ncol(df) + 1):ncol(comb)])$GRS.stat %>%
    round(4) %>%
    format(nsmall = 4)
  
  GRS.pValue <- GRS.test(comb[,2:ncol(df)], comb[,(ncol(df) + 1):ncol(comb)])$GRS.pval %>% sig.stars
  
  return(paste(GRS.score, GRS.pValue, sep = ""))
}

# the smaller the better

GRS.stats <- list()

for (i in 1:length(list.5x5.pf.d)) {
  GRS.stats[[i]] <- rbind(
    list.5x5.pf.d[[i]] %>% GRS.fct(ff3.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ff3.snt.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ff5.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ff5.snt.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ch4.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(ch4.snt.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(full.factors),
    list.5x5.pf.d[[i]] %>% GRS.fct(full.snt.factors)
  )
}

print(GRS.stats)
save(GRS.stats, file = paste(filepath, "GRS.stats.RData", sep = ""))

# p Value of intercepts ####

aai.p.value.fct <- function(model){
  mean(abs(model[[2]]$p.value[which(model[[2]]$term == "(Intercept)")]))
}

aai.p.value.stats <- list()

for (i in 1:1) {
  aai.p.value.stats[[i]] <- rbind(
    list.ff3 %>% aai.p.value.fct,
    list.ff3.snt %>% aai.p.value.fct,
    list.ff5 %>% aai.p.value.fct,
    list.ff5.snt %>% aai.p.value.fct,
    list.ch4 %>% aai.p.value.fct,
    list.ch4.snt %>% aai.p.value.fct,
    list.full %>% aai.p.value.fct,
    list.full.snt]] %>% aai.p.value.fct
  )
}

print(aai.p.value.stats)
save(aai.p.value.stats, file = paste(filepath, "aai.p.value.stats.RData", sep = ""))


# Average absolute intercept over average absolute value of r - NOT WORKING ####
library(matrixStats) # for ColMedians

aai.fct.unscaled <- function(model){
  #browser()
  median(abs(model[[2]]$estimate[which(model[[2]]$term == "(Intercept)")]))
}

aaiOaar.fct <- function(df, model){
  #browser()
  Ri <- df %>% # time series average excess return of portfolio i
    select(-Day) %>%
    as.matrix %>%
    colMedians(na.rm = TRUE)
  Rbar <- median(Ri, na.rm = T) # cross-section average of Ri
  r <- Ri - Rbar
  aai <- model %>% aai.fct.unscaled
  aai / median(abs(r))
}

aaiOaar.stats <- list()

for (i in 1:length(list.ff3)) {
  aaiOaar.stats[[i]] <-
    rbind(
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ff3[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ff3.snt[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ff5[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ff5.snt[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ch4[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.ch4.snt[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.full[[i]]),
      list.5x5.pf.d[[i]] %>% aaiOaar.fct(list.full.snt[[i]])
    )
}

print(aaiOaar.stats)
save(aaiOaar.stats, file = paste(filepath, "aaiOaar.stats.RData", sep = ""))


# Average absolute squared intercept over average absolute squared value of r - NOT WORKING ####

aai.fct.unscaled2 <- function(model){
  #browser()
  median(model[[2]]$estimate[which(model[[2]]$term == "(Intercept)")] ^ 2 -
           model[[2]]$std.error[which(model[[2]]$term == "(Intercept)")] ^ 2) 
}

aaiOaar2.fct <- function(df, model){
  Ri <- df %>% # time series average excess return of portfolio i
    select(-Day) %>%
    as.matrix %>%
    colMedians(na.rm = TRUE)
  Rbar <- median(Ri ^ 2, na.rm = T) # cross-section average of Ri
  r <- (Ri - Rbar) ^ 2
  asqrd <- model %>% aai.fct.unscaled2
  median(asqrd) / median(r)
}

aaiOaar2.stats <- list(
  rbind(list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ff3[[1]]), # size_HML.pf
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ff3.snt[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ff5[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ff5.snt[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ch4[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.ch4.snt[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.full[[1]]),
        list.5x5.pf[[1]] %>% aaiOaar2.fct(list.full.snt[[1]])),
  rbind(list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ff3[[2]]), # size_RMW.pf
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ff3.snt[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ff5[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ff5.snt[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ch4[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.ch4.snt[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.full[[2]]),
        list.5x5.pf[[2]] %>% aaiOaar2.fct(list.full.snt[[2]])),
  rbind(list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ff3[[3]]), # size_CMA.pf
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ff3.snt[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ff5[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ff5.snt[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ch4[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.ch4.snt[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.full[[3]]),
        list.5x5.pf[[3]] %>% aaiOaar2.fct(list.full.snt[[3]])),
  rbind(list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ff3[[4]]), # size_UMD.pf
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ff3.snt[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ff5[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ff5.snt[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ch4[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.ch4.snt[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.full[[4]]),
        list.5x5.pf[[4]] %>% aaiOaar2.fct(list.full.snt[[4]])),
  rbind(list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ff3[[5]]), # size_PMN.pf
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ff3.snt[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ff5[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ff5.snt[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ch4[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.ch4.snt[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.full[[5]]),
        list.5x5.pf[[5]] %>% aaiOaar2.fct(list.full.snt[[5]])))

save(aaiOaar2.stats, file = paste(filepath, "aaiOaar2.stats.RData", sep = ""))


# Average 1-R2 as a similar metric to the previous one ####

#the smaller the better

OnemR2.fct <- function(model){
  mean(abs(model[[4]]$adj.r.squared), na.rm = T)
}

OnemR2.stats <- list()


for (i in 1:1) {
  OnemR2.stats[[i]] <- rbind(
    list.ff3 %>% OnemR2.fct,
    list.ff3.snt %>% OnemR2.fct,
    list.ff5 %>% OnemR2.fct,
    list.ff5.snt %>% OnemR2.fct,
    list.ch4 %>% OnemR2.fct,
    list.ch4.snt %>% OnemR2.fct,
    list.full %>% OnemR2.fct,
    list.full.snt %>% OnemR2.fct
  )
}
print(OnemR2.stats)
save(OnemR2.stats, file = paste(filepath, "OnemR2.stats.RData", sep = ""))
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Regressions of single stocks directly on sentiment  ####

load(paste(filepath, "sorts.RData", sep = ""))
load(paste(filepath, "data.snt.RData", sep = ""))

stock.ret <- data.snt %>%
  select(Date, PERMNO, Ret = retadj.1mn, snt, snt.glo.diff) %>% 
  mutate(snt.squared = snt^2,
         snt.third = snt^3) %>% 
  mutate_at(vars(snt.glo.diff, snt.squared, snt.third), list(~ na.locf(., na.rm = FALSE)))

y <- "stocks" #indicator for WRDS

run_model_stocks_direct_sentiment <- function(df, factors){
    # browser()
  model_formula <- as.formula(paste("Ret ~ snt.glo.diff + ", paste(colnames(select(factors, -Date)), collapse = " + "), sep = ""))
  print(model_formula)
  
  model <- df %>%
    join(factors, by = "Date") %>%
    # filter(complete.cases(.)) %>%
    group_by(PERMNO) %>%
    do(fit.model = lm(model_formula, data = .))
  
  model.coef <- tidy(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.pred <- augment(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.stats <- glance(model, fit.model) # get the coefficients by group in a tidy data_frame
  
  return(list(model,
              model.coef,
              model.pred,
              model.stats))
}

# Define models
ff3.factors <- sorts %>%  select(Date, MktRf, SMB, HML)
ff3.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, PMN) 
ff5.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA)
ff5.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, PMN)
ch4.factors <- sorts %>% select(Date, MktRf, SMB, HML, UMD)
ch4.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, UMD, PMN)
full.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, UMD)
full.snt.factors <- sorts %>% select(Date, MktRf, SMB, HML, RMW, CMA, UMD, PMN)

# Run models
list.ff3 <- stock.ret %>% run_model_stocks(ff3.factors)
list.ff3.snt <- stock.ret %>% run_model_stocks_direct_sentiment(ff3.snt.factors)
list.ff5 <- stock.ret %>% run_model_stocks(ff5.factors)
list.ff5.snt <- stock.ret %>% run_model_stocks_direct_sentiment(ff5.snt.factors)
list.ch4 <- stock.ret %>% run_model_stocks(ch4.factors)
list.ch4.snt <- stock.ret %>% run_model_stocks_direct_sentiment(ch4.snt.factors)
list.full <- stock.ret %>% run_model_stocks(full.factors)
list.full.snt <- stock.ret %>% run_model_stocks_direct_sentiment(full.snt.factors)

# Save models
# save(list.ff3, file = paste(filepath, "list.ff3.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ff3.snt, file = paste(filepath, "list.ff3.snt.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ff5, file = paste(filepath, "list.ff5.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ff5.snt, file = paste(filepath, "list.ff5.snt.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ch4, file = paste(filepath, "list.ch4.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.ch4.snt, file = paste(filepath, "list.ch4.snt.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.full, file = paste(filepath, "list.full.stocks.sorts.glo.diff.RData", sep = ""))
# save(list.full.snt, file = paste(filepath, "list.full.snt.stocks.sorts.glo.diff.RData", sep = ""))

# load models if needed
# load(file = paste(filepath, "list.ff3.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ff3.snt.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ff5.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ff5.snt.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ch4.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.ch4.snt.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.full.stocks.RData", sep = ""))
# load(file = paste(filepath, "list.full.snt.stocks.RData", sep = ""))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Neural Network ####
filepath = getwd()

library(keras) # for deep learning
library(tidyverse) # general utility functions
library(caret) # machine learning utility functions
library(dplyr)

# Load data
load(paste(filepath, "/sorts.RData", sep = ""))
load(paste(filepath, "/list.5x5.pf.d.RData", sep = ""))

# load(paste(filepath, "sorts.RData", sep = ""))
# load(paste(filepath, "list.5x5.pf.d.RData", sep = ""))

factor.portfolios <- list.5x5.pf.d %>% 
  list_df2df() %>% 
  gather(key = port, value = ret, -c(Date, X1)) %>% 
  as_tibble()

data <- sorts %>% 
  mutate(Date = yearweek(Date)) %>% 
  select(Date, MktRf, SMB, HML, RMW, CMA, UMD, PMN) %>% 
  inner_join(factor.portfolios, by = "Date") %>% 
  mutate(Date = yearweek(Date)) %>% 
  group_by(X1, port) 

# reverse scaling for later 
# data2 <- t(t(data) * std + mean)

# Create datasets for training, validation and test
no.train <- 0.50
no.valid <- 0.25 
no.test <- 0.25

min.train <- unique(data$Date)[1]
max.train <- unique(data$Date)[length(unique(data$Date))*no.train]
min.valid <- unique(data$Date)[length(unique(data$Date))*no.train + 1]
max.valid <- unique(data$Date)[length(unique(data$Date))*(no.train + no.valid)]
min.test <-  unique(data$Date)[length(unique(data$Date))*(no.train + no.valid) + 1]
max.test <-  unique(data$Date)[length(unique(data$Date))]

train.data <- data %>% 
  filter(Date >= min.train & Date <= max.train ) %>%
  na.omit

valid.data <- data %>% 
  filter(Date >= min.valid & Date <= max.valid ) %>%
  na.omit

test.data <- data %>% 
  filter(Date >= min.test & Date <= max.test ) %>%
  na.omit

# No pre-processing required as all time series are returns on the same scale
# mean <- apply(train.dataset, 2, mean, na.rm = T)
# std <- apply(train.dataset, 2, sd, na.rm = T)
# data <- scale(dataset, center = mean, scale = std)

# Generator function

generator <- function(data, lookback, delay, min_index, max_index,
                      shuffle = FALSE, batch_size = 128, step = 1) {
   # browser()
  if (is.null(max_index))
    max_index <- nrow(data) - delay - 1
  i <- min_index + lookback
  function() {
    if (shuffle) {
      rows <- sample(c((min_index + lookback):max_index), size = batch_size)
    } else {
      if (i + batch_size >= max_index)
        i <<- min_index + lookback
      rows <- c(i:min(i + batch_size - 1, max_index))
      i <<- i + length(rows)
    }
    
    samples <- array(0, dim = c(length(rows),
                                lookback / step,
                                dim(data)[[-1]]))
    targets <- array(0, dim = c(length(rows)))
    print(dim(samples))
    
    for (j in 1:length(rows)) {
      indices <- seq(rows[[j]] - lookback, rows[[j]] - 1,
                     length.out = dim(samples)[[2]])
      samples[j,,] <- data[indices,]
      targets[[j]] <- data[rows[[j]] + delay, ncol(data)]
    }           
    list(samples, targets)
  }
}

lookback <- 26 # How many timesteps back the input data should go? weekly: 4x3 for a quarter
step <- 1 # The period, in timesteps, at which you sample data
delay <- 1 # How many timesteps in the future the target should be? next week
batch_size <- 256

train_gen <- generator(
  data,
  lookback = lookback,
  delay = delay,
  min_index = 1,
  max_index = max.train,
  step = step, 
  batch_size = batch_size
)

val_gen = generator(
  data,
  lookback = lookback,
  delay = delay,
  min_index = min.valid,
  max_index = max.valid,
  step = step,
  batch_size = batch_size
)

test_gen <- generator(
  data,
  lookback = lookback,
  delay = delay,
  min_index = min.test,
  max_index = NULL,
  step = step,
  batch_size = batch_size
)

# How many steps to draw from val_gen in order to see the entire validation set
val_steps <- (max.valid - min.valid - lookback) #/ batch_size

# How many steps to draw from test_gen in order to see the entire test set
test_steps <- (nrow(dataset) - min.test - lookback) #/ batch_size

# set a random seed for reproducability
set.seed(123)
library(keras)
library(tensorflow)
library(reticulate)
library(sjPlot)
# use_virtualenv('~/.virtualenvs/R_t_27') on WRDS cloud

r2_keras <- custom_metric("r2_keras", function(y_true, y_pred){
  SS_res =  k_sum(k_square(y_true - y_pred)) 
  SS_tot = k_sum(k_square(y_true - k_mean(y_true))) 
  return ( 1 - SS_res/(SS_tot))
  # SS_res =  sum((y_true - y_pred)^2) 
  # SS_tot = sum((y_true - mean(y_true, na.rm = T))^2) 
  # return( 1 - SS_res/(SS_tot) )
})

# set global default to always show metrics
options(keras.view_metrics = TRUE)

# Linear benchmark model ####

lin.model <- train.data %>%
  group_by(X1, port) %>% 
  do(fit.model = lm(ret ~ MktRf + SMB + HML + RMW + CMA + UMD + PMN, data = .))

eval_fct <- function(model){
  # browser()
  pred <- model$.fitted
  act <- model$ret
  SS.res <- sum((pred - act)^2)
  SS.tot <- sum((act - mean(act, na.rm = TRUE))^2)
  r2 <- 1 - SS.res / SS.tot
  
  mse = mean((pred - act)^2, na.rm = TRUE)
  
  return(c(r2, mse))
}

lin.model %>% tidy(fit.model)
lin.model %>% glance(fit.model) %>% ungroup() %>% summarize_at(vars(adj.r.squared), list(~ mean(.,na.rm = T)))
lin.model %>% augment(fit.model) %>% lapply(eval_fct())

lin.model <- lm(ret ~ MktRf + SMB + HML + RMW + CMA + UMD, data = as.data.frame(train.data))

(results <- eval_fct(lin.model$fitted.values, train.data[,ncol(train.data)]))

predictions <- predict(lin.model, valid.data)
(results <-  eval_fct(predictions, valid.data[,ncol(valid.data)]))

predictions <- predict(lin.model, as.data.frame(test.data))
(results <-  eval_fct(predictions, test.data[,ncol(test.data)]))

# Simple Feed forward NN using AMORE ####

library(AMORE)

inputs <- sorts %>% 
  mutate(Date = yearweek(Date)) %>% 
  select(MktRf, SMB, HML, RMW, CMA, UMD, PMN) %>% 
  as.matrix()

expect_equal(dim(inputs), c(1044, 7))

targets <- list.5x5.pf.d[[1]] %>% 
  select(S1.P1) %>% 
  as.matrix()

expect_equal(dim(targets), c(1044, 1))

net <- newff(n.neurons = c(1, 8, 1), 
             learning.rate.global = 1e-100, 
             momentum.global = 0.0000000001,
             error.criterium = "LMS", 
             hidden.layer = "purelin", 
             output.layer = "purelin", 
             method = "ADAPTgdwm") 

ffwd.nn <- train(net, inputs, targets, report = T, show.step = 32, n.shows = 5)

lin.model <- train.data %>%
  group_by(X1, port) %>% 
  do(ff = train(net, MktRf, ret, error.criterium="LMS", report=TRUE, show.step=100, n.shows=5))


result <- train(net, MktRf, ret, error.criterium="LMS", report=TRUE, show.step=100, n.shows=5 )

# Simple Linear model ####

model <- keras_model_sequential() %>% 
  layer_dense(units = 1, activation = "linear") 

model %>% compile(
  optimizer = optimizer_rmsprop(),
  loss = "mse",
  metrics = r2_keras)

linear.history <- model %>% fit_generator(
  train_gen,
  steps_per_epoch = 500,
  epochs = 5,
  validation_data = val_gen,
  validation_steps = val_steps)

linear.history %>% plot() + geom_line()
save(linear.history, file = "linear.history.RData")

# plot_model(linear.history)
# get_weights(linear.history)


# Basic machine learning approach ####
model <- keras_model_sequential() %>% 
  layer_flatten(input_shape = c(lookback / step, dim(data)[-1])) %>% 
  layer_dense(units = 32, activation = "relu") %>% 
  layer_dense(units = 1)

model %>% compile(
  optimizer = optimizer_rmsprop(),
  loss = "mse",
  metrics = r2_keras)

base.history <- model %>% fit_generator(
  train_gen,
  steps_per_epoch = 500,
  epochs = 5,
  validation_data = val_gen,
  validation_steps = val_steps)

base.history %>% plot() + geom_line()
save(base.history, file = "base.history.RData")

# A first recurrent baseline ####

model <- keras_model_sequential() %>% 
  layer_gru(units = 32, input_shape = list(NULL, dim(data)[[-1]])) %>% 
  layer_dense(units = 1)

model %>% compile(
  optimizer = optimizer_rmsprop(),
  loss = "mse",
  metrics = r2_keras)

rnn.history <- model %>% fit_generator(
  train_gen,
  steps_per_epoch = 500,
  epochs = 5,
  validation_data = val_gen,
  validation_steps = val_steps)

rnn.history %>% plot() + geom_line()
save(rnn.history, file = "rnn.history.RData")

# Using recurrent dropout to fight overfitting ####

model <- keras_model_sequential() %>% 
  layer_gru(units = 32, dropout = 0.2, recurrent_dropout = 0.2,
            input_shape = list(NULL, dim(data)[[-1]])) %>% 
  layer_dense(units = 1)

model %>% compile(
  optimizer = optimizer_rmsprop(),
  loss = "mse",
  metrics = r2_keras)

rec.drop.history <- model %>% fit_generator(
  train_gen,
  steps_per_epoch = 500,
  epochs = 5,
  validation_data = val_gen,
  validation_steps = val_steps)

rec.drop.history %>% plot()  + geom_line()
save(rec.drop.history, file = "rec.drop.history.RData")

# Stacking recurrent layers ####

model <- keras_model_sequential() %>% 
  layer_gru(units = 32, 
            dropout = 0.1, 
            recurrent_dropout = 0.5,
            return_sequences = TRUE,
            input_shape = list(NULL, dim(data)[[-1]])) %>% 
  layer_gru(units = 64, activation = "relu",
            dropout = 0.1,
            recurrent_dropout = 0.5) %>% 
  layer_dense(units = 1)

model %>% compile(
  optimizer = optimizer_rmsprop(),
  loss = "mse",
  metrics = r2_keras)

stacked.history <- model %>% fit_generator(
  train_gen,
  steps_per_epoch = 500,
  epochs = 5,
  validation_data = val_gen,
  validation_steps = val_steps)

stacked.history %>% plot() + geom_line()
save(stacked.history, file = "stacked.history.RData")

# LSTM ####

model = keras_model_sequential() %>% 
  layer_lstm(units = 8, input_shape = c(lookback / step, dim(dataset)[-1]), activation = "relu") %>%  
  layer_dense(units = 4, activation = "relu") %>% 
  layer_dense(units = 2) %>% 
  layer_dense(units = 1)

model %>% compile(
  optimizer = optimizer_rmsprop(),
  loss = "mse",
  metrics = r2_keras)

lstm.r2.history <- model %>% fit_generator(
  train_gen,
  steps_per_epoch = 500,
  epochs = 20,
  validation_data = val_gen,
  validation_steps = val_steps)

lstm.r2.history %>%  plot() + geom_line()
save(lstm.r2.history, file = "lstm.r2.history.RData")

# Evaluate and predict model ####
(results <- model %>% evaluate_generator(test_gen, steps = test_steps))
predictions <- model %>% predict_generator(test_gen, steps = test_steps)
plot(predictions)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Descriptive statistics on sentiment - Daily #####

# Merge industry classification to sentiment data

load(paste(filepath, "data.snt.d.RData", sep = ""))

# sentiment by sector 
data.snt.sector.d <- data.snt.d %>% 
  select(PERMNO, naics) %>% 
  group_by(PERMNO) %>% 
  na.locf %>% 
  na.locf(fromLast = TRUE) %>% 
  merge(data.snt.d, by = c("PERMNO", "naics")) %>% 
  # for myseterious reason fill does not work anymore so below code is commented out
  # data.snt.d %>% 
  # group_by(PERMNO) %>% 
  # fill(naics) %>% # fill NAs in direction
  # fill(naics, .direction = "up") %>% # fill NAs in reverse direction
  ungroup %>%
  mutate(Var = substr(naics,1,2)) %>%
  group_by(Var) %>%
  summarize(Mean = mean(aggr.snt, na.rm = TRUE) *100,
            Median = median(aggr.snt, na.rm = TRUE)*100,
            SD = sd(aggr.snt, na.rm = TRUE)*100,
            N = n_distinct(PERMNO),
            t.test = t.test(aggr.snt)$p.value)

save(data.snt.sector.d, file = paste(filepath, "data.snt.sector.d.RData", sep = ""))

# Get sector names
naics.codes <- read.csv("NAICS_codes.csv")

data.snt.sector.d <- data.snt.sector.d %>% 
  merge(naics.codes, by.x = "Var", by.y = "Sector", all.x = TRUE) %>%
  select(Var, Name, Mean, Median, SD, N)

save(data.snt.sector.d, file = paste(filepath, "data.snt.sector.d.RData", sep = ""))

# define grouping function

Form_sentiment_summary_by_group <- function(main, var) { # streamlined version
  # forms 5x5 (size x specificed-characteristc) and forms the 25 portfolios
  # variables broken by quantiles (breakpoints uses NYSE data only)
  # requires Date and exchcd
  # outputs portfolio *excess* returns for each period,
  # browser()
  main.cln <- main %>%
    select(Date, PERMNO, exchcd, !!var, aggr.snt) 
  
  Bkpts.NYSE <- main.cln %>% # create size and var breakpoints based on NYSE stocks only
    group_by(Date) %>%
    summarize(var.P80 = quantile(!!var, probs = .8, na.rm = TRUE),
              var.P60 = quantile(!!var, probs = .6, na.rm = TRUE),
              var.P40 = quantile(!!var, probs = .4, na.rm = TRUE),
              var.P20 = quantile(!!var, probs = .2, na.rm = TRUE))
  
  # calculate size and var portfolio returns
  main.rank <- main.cln %>%
    merge(Bkpts.NYSE, by = "Date", all.x = TRUE) %>%
    mutate(Var = ifelse(!!var<var.P20, "Q1", ifelse(!!var<var.P40, "Q2", ifelse(!!var<var.P60, "Q3", ifelse(!!var<var.P80, "Q4", "Q5"))))) #small big
  
  Sum <- main.rank %>% 
    group_by(Var) %>%
    summarize(Mean = mean(aggr.snt, na.rm = TRUE)*100,
              Median = median(aggr.snt, na.rm = TRUE)*100,
              SD = sd(aggr.snt, na.rm = TRUE)*100,
              N = n_distinct(PERMNO))
  return(Sum)
}

# sentiment by liquidity 

data.snt.liq.d <- data.snt.d %>% 
  mutate(liq = vol / shrout) %>%
  Form_sentiment_summary_by_group(quo(liq)) %>%
  mutate(Name = Var) %>%
  select(Var, Name, Mean, Median, SD, N)

save(data.snt.liq.d, file = paste(filepath, "data.snt.liq.d.RData", sep = ""))

# sentiment by size 

data.snt.size.d <- Form_sentiment_summary_by_group(data.snt.d, quo(ME)) %>%
  mutate(Name = Var) %>%
  select(Var, Name, Mean, Median, SD, N)

save(data.snt.size.d, file = paste(filepath, "data.snt.size.d.RData", sep = ""))

# sentiment by exchange
data.snt.exchg.d <- data.snt.d %>% 
  # select(PERMNO, exchcd) %>% 
  # group_by(PERMNO) %>% 
  # na.locf %>% 
  # na.locf(fromLast = TRUE) %>% 
  # merge(data.snt.d, by = c("PERMNO", "exchcd")) %>% 
  # #group_by(PERMNO) %>% 
  #fill(exchcd) %>% # fill NAs in direction
  #fill(exchcd, .direction = "up") %>% # fill NAs in reverse direction
  ungroup %>%
  mutate(Var = exchcd) %>%
  group_by(Var) %>%
  summarize(Mean = mean(aggr.snt, na.rm = TRUE)*100,
            Median = median(aggr.snt, na.rm = TRUE)*100,
            SD = sd(aggr.snt, na.rm = TRUE)*100,
            N = n_distinct(PERMNO))

save(data.snt.exchg.d, file = paste(filepath, "data.snt.exchg.d.RData", sep = ""))

# Create exchange code table
exchcd.codes <- as.data.frame(
  cbind(
    c(1,2,3,NA),
    c("NYSE", "AMEX","NASDAQ","Others")))

colnames(exchcd.codes) <- c("Var", "Name")

data.snt.exchg.d <- data.snt.exchg.d %>% 
  merge(exchcd.codes, by = "Var", all.x = TRUE) %>%
  select(Var, Name, Mean, Median, SD, N)

save(data.snt.exchg.d, file = paste(filepath, "data.snt.exchg.d.RData", sep = ""))

# Aggregate results into Latex table
snt.summary.d <- list(data.snt.sector.d, data.snt.liq.d, data.snt.size.d, data.snt.exchg.d)

attr(snt.summary.d, "subheadings") <- c("Panel A: Sentiment by Sector",
                                        "Panel B: Sentiment by Liquidity",
                                        "Panel C: Sentiment by Size",
                                        "Panel D: Sentiment by Exchange")

save(snt.summary.d, file = paste(filepath, "snt.summary.d.RData", sep = ""))

snt.summary.xtL.d <- snt.summary.d %>% 
  xtableList(tabular.environment = "tabularx",
             colNames = FALSE,
             caption = "Descriptive Statistics of Sentiment by Group",
             include.rownames = FALSE,
             label = "table_snt_desc")

save(snt.summary.xtL.d, file = paste(filepath, "snt.summary.xtL.d.RData", sep = ""))

print.xtableList(snt.summary.xtL.d,
                 tabular.environment = "tabularx",
                 file = "table_snt.desc.d.tex",
                 size = "footnotesize",
                 caption.placement = "top",
                 include.rownames = FALSE)

# Needs to be copied manually to the file on WRDS...
# since the file generation is not working for some reason

# Plot sector distribution

barplot(data.snt.sector.d$Mean, names.arg = data.snt.sector.d$Var)
barplot(data.snt.sector.d$SD, names.arg = data.snt.sector.d$Var)

# Next would be nicer but does not work on WRDS
ggplot(data.snt.sector.d, aes(x = SD, y = Mean, fill = Var)) + 
  geom_bar(stat = "identity", position = "identity")

rm(data.snt.sector.d, data.snt.liq.d, data.snt.size.d, data.snt.exchg.d, snt.summary.d, snt.summary.xtL.d, naics.codes)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####

# Check against given FF values ####
# copy from above

start <- 1997
end <- 2018
Compare_Two_Vectors <- function(v1, v2, sqnc = 1) { # omits DATE option
  # takes two dataframes where each has a Date and another var column
  # compares the two var columns for similarity
  # v1.date, v2.date denotes col name of date which must be yearmon
  # sqnc affects the plot, e.g. if  = 3 then plot every third point (so not too crowded)
  # will not work if missing observations in between for one dataframe since cor needs exact same length
  
  # obtain common time segment among two vectors
  lo.date <- max(v1[["Date"]][min(which(!is.na(v1[[colnames(v1)[2]]])))],
                 v2[["Date"]][min(which(!is.na(v2[[colnames(v2)[2]]])))])
  hi.date <- min(v1[["Date"]][max(which(!is.na(v1[[colnames(v1)[2]]])))],
                 v2[["Date"]][max(which(!is.na(v2[[colnames(v2)[2]]])))])
  v1.trim <- subset(v1, lo.date <=  Day & Day <=  hi.date)
  v2.trim <- subset(v2, lo.date <=  Day & Day <=  hi.date)
  
  df <- v1.trim %>%
    merge(v2.trim, by = "Date", all.x = TRUE)
  
  print("correlation")
  print(cor(df[[colnames(df)[2]]], df[[colnames(df)[3]]], use = "complete.obs"))
  print("v1.trim mean")
  print(mean(df[[colnames(df)[2]]], na.rm = TRUE))
  print("v2.trim mean")
  print(mean(df[[colnames(df)[3]]], na.rm = TRUE))
  print("means: v1.trim-v2.trim")
  print(mean(df[[colnames(df)[2]]], na.rm = TRUE) - mean(df[[colnames(df)[3]]], na.rm = TRUE))
  # plot(df[["Date"]][seq(1,nrow(df),sqnc)],
  #      df[[colnames(df)[2]]][seq(1,nrow(df),sqnc)], type = "l", xlab = "Day", ylab = "Returns (%)")
  # lines(df[["Date"]][seq(1,nrow(df),sqnc)],
  #       df[[colnames(df)[3]]][seq(1,nrow(df),sqnc)], type = "l", col = "blue")
  # legend("topright", c("1", "2"), lty = 1, col = c("black","blue"))
}

check_factors <- function(df, benchmark, start, end) {
  # browser()
  benchmark <- benchmark %>% 
    filter(year(Day) >=  start & year(Day) <=  end)
  
  print(Compare_Two_Vectors(select(df, Day, Mkt), select(benchmark, Day, FFMkt = Mkt), sqnc = 12))
  print(Compare_Two_Vectors(select(df, Day, SMB), select(benchmark, Day, FFSMB = SMB), sqnc = 12))
  print(Compare_Two_Vectors(select(df, Day, HML), select(benchmark, Day, FFHML = HML), sqnc = 12))
  print(Compare_Two_Vectors(select(df, Day, RMW), select(benchmark, Day, FFRMW = RMW), sqnc = 12))
  print(Compare_Two_Vectors(select(df, Day, CMA), select(benchmark, Day, FFCMA = CMA), sqnc = 12))
  print(Compare_Two_Vectors(select(df, Day, UMD), select(benchmark, Day, FFUMD = UMD), sqnc = 12))
}

sorts %>% check_factors(dt.FF6, start, end)

rm(dt.FF6, snt.sorts.2x2x2x2x2x2, sorts.2x3, snt.sorts.2x2x2x2x2x2.diff, snt.sorts.2x2x2x2x2x2.lvl, sorts.2x3.diff, sorts.2x3.lvl)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Run statistics on factors  ####
load(paste(filepath, "snt.sorts.2x2x2x2x2x2.RData", sep = ""))
load(paste(filepath, "sorts.2x3.RData", sep = ""))
load(paste(filepath, "dt.FF6.RData", sep = ""))

snt.sorts <- dt.FF6 %>%
  select(Date, RF, MktRF) %>%
  merge(snt.sorts.2x2x2x2x2x2, by = "Date", all.y = TRUE)

sorts <- dt.FF6 %>%
  select(Date, RF, MktRF) %>%
  merge(sorts.2x3, by = "Date", all.y = TRUE) 

factor_sum_stats <- function(df) {
  df.scaled <- df %>% select(-Date) * 10000 # 100 to get percentage returns, 10000 for bps
  
  snt.factors.mean <- df.scaled %>%
    summarize_all(list(~ mean(., na.rm = TRUE)))
  rownames(snt.factors.mean) <- "Mean"
  
  snt.factors.sd <- df.scaled %>%
    summarize_all(list(~ sd(., na.rm = TRUE)))  
  rownames(snt.factors.sd) <- "Std. dev"
  
  snt.factors.t.test <- df.scaled %>%
    summarize_all(list(~ t.test(.)[1])) 
  rownames(snt.factors.t.test) <- "t-Statistic"
  
  #only for reference
  snt.factors.p.value <- df.scaled %>%
    summarize_all(list(~ t.test(.)[3])) 
  rownames(snt.factors.p.value) <- "p.value"
  print(snt.factors.p.value)
  
  snt.factors.corr <- df.scaled %>%
    as.matrix() %>%
    rcorr(type = "spearman")
  
  snt.factors.corr.r <- snt.factors.corr$r
  snt.factors.corr.r[lower.tri(snt.factors.corr.r)] <- NA
  
  snt.factors.corr.P <- snt.factors.corr$P
  snt.factors.corr.P[lower.tri(snt.factors.corr.P, diag = TRUE)] <- NA
  
  # Create Latex table
  snt.table_factors_sum <- list(snt.factors.mean,
                                snt.factors.sd, 
                                snt.factors.t.test,
                                snt.factors.corr.r,
                                snt.factors.corr.P)
  
  save(snt.table_factors_sum, file = paste(filepath, "snt.table_factors_sum.RData", sep = ""))
  
  attr(snt.table_factors_sum, "subheadings") <- c("Panel A: Average, standard deviation and one-sample t-statistics for monthly returns",
                                                  "REMOVE",
                                                  "REMOVE ",
                                                  "Panel B: Correlation between different factors",
                                                  "Panel C: P-value of correlations")
  
  snt.table_factors_sum.xtL <- snt.table_factors_sum %>% 
    xtableList(colNames = FALSE,
               caption = "Summary Statistics for Factor Returns",
               include.rownames = TRUE,
               label = "snt.table_factors_sum",
               digits = 2)
  
  save(snt.table_factors_sum.xtL, file = paste(filepath, "snt.table_factors_sum.xtL.RData", sep = ""))
  
  print.xtableList(snt.table_factors_sum.xtL,
                   tabular.environment = "tabularx",
                   file = "snt.table_factors_sum.tex",
                   size = "footnotesize",
                   caption.placement = "top",
                   width = "\\textwidth",
                   include.rownames = TRUE)
  
  rm(snt.factors.mean, snt.factors.sd, snt.factors.t.test, snt.factors.p.value, snt.factors.corr.r, snt.factors.corr.P, snt.table_factors_sum, snt.table_factors_sum.xtL)
}

snt.sorts %>% factor_sum_stats
sorts %>% 
  select(Date, MktRf, SMB, HML, RMW, CMA, UMD, PMN, PMNP, PMNN) %>% 
  factor_sum_stats

rm(snt.sorts.2x2x2x2x2x2, dt.FF6, sorts.2x3, sorts, snt.sorts)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Run regressions of one factor against the others ####

run_model_on_factors <- function(factors){
  #browser()
  model_formula <- as.formula(paste(colnames(factors)[1], " ~ ", paste(colnames(factors)[2:ncol(factors)], collapse = " + "), sep = " "))
  
  model <- factors %>% do(fit.model = lm(model_formula, data = .))
  
  model.coef <- tidy(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.pred <- augment(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.stats <- glance(model, fit.model) # get the coefficients by group in a tidy data_frame
  
  return(list(model,
              model.coef,
              model.pred,
              model.stats))
}

MktRf <- snt.sorts %>% select(MktRf,      SMB, HML, RMW, CMA, UMD, PMN) %>% run_model_on_factors
SMB   <- snt.sorts %>% select(SMB, MktRf,      HML, RMW, CMA, UMD, PMN) %>% run_model_on_factors
HML   <- snt.sorts %>% select(HML, MktRf, SMB,      RMW, CMA, UMD, PMN) %>% run_model_on_factors
RMW   <- snt.sorts %>% select(RMW, MktRf, SMB, HML,      CMA, UMD, PMN) %>% run_model_on_factors
CMA   <- snt.sorts %>% select(CMA, MktRf, SMB, HML, RMW,      UMD, PMN) %>% run_model_on_factors
UMD   <- snt.sorts %>% select(UMD, MktRf, SMB, HML, RMW, CMA,      PMN) %>% run_model_on_factors
PMN   <- snt.sorts %>% select(PMN, MktRf, SMB, HML, RMW, CMA, UMD,    ) %>% run_model_on_factors

regressions_on_factors <- list(rbind(cbind(t(MktRf[[2]]$estimate), MktRf[[4]]$adj.r.squared),
                                     cbind(t(MktRf[[2]]$statistic), NA)),
                               rbind(cbind(t(SMB[[2]]$estimate), SMB[[4]]$adj.r.squared),
                                     cbind(t(SMB[[2]]$statistic), NA)),
                               rbind(cbind(t(HML[[2]]$estimate), HML[[4]]$adj.r.squared),
                                     cbind(t(HML[[2]]$statistic), NA)),
                               rbind(cbind(t(RMW[[2]]$estimate), RMW[[4]]$adj.r.squared),
                                     cbind(t(RMW[[2]]$statistic), NA)),
                               rbind(cbind(t(CMA[[2]]$estimate), CMA[[4]]$adj.r.squared),
                                     cbind(t(CMA[[2]]$statistic), NA)),
                               rbind(cbind(t(UMD[[2]]$estimate), UMD[[4]]$adj.r.squared),
                                     cbind(t(UMD[[2]]$statistic), NA)),
                               rbind(cbind(t(PMN[[2]]$estimate), PMN[[4]]$adj.r.squared),
                                     cbind(t(PMN[[2]]$statistic), NA)))

names(regressions_on_factors) <- c("MktRf",
                                   "SMB",
                                   "HML",
                                   "RMW",
                                   "CMA",
                                   "UMD",
                                   "PMN")

colnames(regressions_on_factors[[1]]) <- c(MktRf[[2]]$term, "R^2")
colnames(regressions_on_factors[[2]]) <- c(SMB[[2]]$term, "R^2")
colnames(regressions_on_factors[[3]]) <- c(HML[[2]]$term, "R^2")
colnames(regressions_on_factors[[4]]) <- c(RMW[[2]]$term, "R^2")
colnames(regressions_on_factors[[5]]) <- c(CMA[[2]]$term, "R^2")
colnames(regressions_on_factors[[6]]) <- c(UMD[[2]]$term, "R^2")
colnames(regressions_on_factors[[7]]) <- c(PMN[[2]]$term, "R^2")

regressions_on_factors <- regressions_on_factors$MktRf %>% as.data.frame %>%
  bind_rows(as.data.frame(regressions_on_factors$SMB)) %>%
  bind_rows(as.data.frame(regressions_on_factors$HML)) %>%
  bind_rows(as.data.frame(regressions_on_factors$RMW)) %>%
  bind_rows(as.data.frame(regressions_on_factors$CMA)) %>%
  bind_rows(as.data.frame(regressions_on_factors$UMD)) %>%
  bind_rows(as.data.frame(regressions_on_factors$PMN)) %>%
  select("(Intercept)", MktRf, SMB, HML, RMW, CMA, UMD, PMN, "R^2")

regressions_on_factors <- list(regressions_on_factors[1:2,],
                               regressions_on_factors[3:4,],
                               regressions_on_factors[5:6,],
                               regressions_on_factors[7:8,],
                               regressions_on_factors[9:10,],
                               regressions_on_factors[11:12,],
                               regressions_on_factors[13:14,])

rownames(regressions_on_factors[[c(1)]]) <- 
  rownames(regressions_on_factors[[c(2)]]) <-
  rownames(regressions_on_factors[[c(3)]]) <-
  rownames(regressions_on_factors[[c(4)]]) <-
  rownames(regressions_on_factors[[c(5)]]) <-
  rownames(regressions_on_factors[[c(6)]]) <-
  rownames(regressions_on_factors[[c(7)]]) <- c("Coef",
                                                "t-statistic")

names(regressions_on_factors) <- c("MktRf",
                                   "SMB",
                                   "HML",
                                   "RMW",
                                   "CMA",
                                   "UMD",
                                   "PMN")

attr(regressions_on_factors, "subheadings") <- c("MktRf",
                                                 "SMB",
                                                 "HML",
                                                 "RMW",
                                                 "CMA",
                                                 "UMD",
                                                 "PMN")

save(regressions_on_factors, file = paste(filepath, "regressions_on_factors.RData", sep = ""))

regressions_on_factors.xtL <- regressions_on_factors %>% 
  xtableList(colNames = FALSE,
             caption = "Regressions on Factors",
             include.rownames = TRUE,
             include.colnames = TRUE,
             label = "regressions_on_factors",
             digits = 2)

save(regressions_on_factors.xtL, file = paste(filepath, "regressions_on_factors.xtL.RData", sep = ""))

print.xtableList(regressions_on_factors.xtL,
                 tabular.environment = "tabularx",
                 file = "sum.stats.tex",
                 size = "footnotesize",
                 caption.placement = "top",
                 width = "\\textwidth",
                 include.rownames = TRUE)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Check whether our model solves the issue of extreme growth stocks #####

# function copied from above
run_model <- function(df, factors){
  #browser()
  model_formula <- as.formula(paste("Ret ~ ", paste(colnames(select(factors, -Day)), collapse = " + "), sep = ""))
  
  model <- df %>%
    gather(key = "port", value = "Ret", -Day) %>%
    merge(factors, by = "Day") %>%
    group_by(port) %>%
    do(fit.model = lm(model_formula, data = .))
  
  model.coef <- tidy(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.pred <- augment(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.stats <- glance(model, fit.model) # get the coefficients by group in a tidy data_frame
  
  return(list(model,
              model.coef,
              model.pred,
              model.stats))
}

# load data
load(paste(filepath, "snt.sorts.RData", sep = ""))
load(paste(filepath, "list.5x5.pf.RData", sep = ""))

# determine fully-fledged model incl. sentiment
full.snt.factors <- snt.sorts %>%
  select(Day, MktRf, SMB, HML, RMW, CMA, UMD, PMN)

# run model incl. sentiment
list.full.snt <- run_model(list.5x5.pf[[1]], full.snt.factors)

# extract coefficient for intercept
intercept.snt <- list.full.snt[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(estimate) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE) * 10000

# extract t-statistic for intercept
statistic.intercept <- list.full.snt[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(statistic) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE)

# merge coefficient and intercept
regression_list.snt <- cbind(intercept.snt, NA, statistic.intercept)
rm(intercept.snt,statistic.intercept)

# define factors for fully-fledged model
full.factors <- snt.sorts %>%
  select(Day, MktRf, SMB, HML)#, RMW, CMA, UMD)

# run model without sentiment
list.full <- run_model(list.5x5.pf[[1]], full.factors)

# extract coefficient for intercept
intercept <- list.full[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(estimate) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE) * 10000

# extract t-statistic for intercept
statistic <- list.full[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(statistic) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE)

# merge coefficient and intercept
regression_list <- cbind(intercept, NA, statistic)
rm(intercept, statistic)

# extract coefficient for sentiment
estimate.snt <- list.full.snt[[2]] %>%
  filter(term == "PMN") %>%
  ungroup %>%
  select(estimate) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE) * 10000

# extract t-statistic for sentiment
statistic.snt <- list.full.snt[[2]] %>%
  filter(term == "PMN") %>%
  ungroup %>%
  select(statistic) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE)

# merge coefficient and intercept
regression_list.PMN <- cbind(estimate.snt, NA, statistic.snt)
rm(estimate.snt, statistic.snt)


# Check whether our model solves the issue of extreme investment stocks #####

# function copied from above
run_model <- function(df, factors){
  #browser()
  model_formula <- as.formula(paste("Ret ~ ", paste(colnames(select(factors, -Day)), collapse = " + "), sep = ""))
  
  model <- df %>%
    gather(key = "port", value = "Ret", -Day) %>%
    merge(factors, by = "Day") %>%
    group_by(port) %>%
    do(fit.model = lm(model_formula, data = .))
  
  model.coef <- tidy(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.pred <- augment(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.stats <- glance(model, fit.model) # get the coefficients by group in a tidy data_frame
  
  return(list(model,
              model.coef,
              model.pred,
              model.stats))
}

# load data
load(paste(filepath, "snt.sorts.RData", sep = ""))
load(paste(filepath, "list.5x5.pf.RData", sep = ""))

# determine fully-fledged model incl. sentiment
full.snt.factors <- snt.sorts %>%
  select(Day, MktRf, SMB, HML, RMW, CMA, PMN)

# run model incl. sentiment
list.full.snt <- run_model(list.5x5.pf[[3]], full.snt.factors)

# extract coefficient for intercept
intercept.snt <- list.full.snt[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(estimate) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE) * 10000

# extract t-statistic for intercept
statistic.intercept <- list.full.snt[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(statistic) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE)

# merge coefficient and intercept
regression_list.snt <- cbind(intercept.snt, NA, statistic.intercept)
rm(intercept.snt,statistic.intercept)

# define factors for fully-fledged model
full.factors <- snt.sorts %>%
  select(Day, MktRf, SMB, HML, RMW, CMA)

# run model without sentiment
list.full <- run_model(list.5x5.pf[[3]], full.factors)

# extract coefficient for intercept
intercept <- list.full[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(estimate) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE) * 10000

# extract t-statistic for intercept
statistic <- list.full[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(statistic) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE)

# merge coefficient and intercept
regression_list <- cbind(intercept, NA, statistic)
rm(intercept, statistic)

# extract coefficient for sentiment
estimate.snt <- list.full.snt[[2]] %>%
  filter(term == "PMN") %>%
  ungroup %>%
  select(estimate) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE) * 10000

# extract t-statistic for sentiment
statistic.snt <- list.full.snt[[2]] %>%
  filter(term == "PMN") %>%
  ungroup %>%
  select(statistic) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE)

# merge coefficient and intercept
regression_list.PMN <- cbind(estimate.snt, NA, statistic.snt)
rm(estimate.snt, statistic.snt)


# Check how fundamental factors work for portfolios sorted on sentiment #####

# function copied from above
run_model <- function(df, factors){
  #browser()
  model_formula <- as.formula(paste("Ret ~ ", paste(colnames(select(factors, -Day)), collapse = " + "), sep = ""))
  
  model <- df %>%
    gather(key = "port", value = "Ret", -Day) %>%
    merge(factors, by = "Day") %>%
    group_by(port) %>%
    do(fit.model = lm(model_formula, data = .))
  
  model.coef <- tidy(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.pred <- augment(model, fit.model) # get the coefficients by group in a tidy data_frame
  model.stats <- glance(model, fit.model) # get the coefficients by group in a tidy data_frame
  
  return(list(model,
              model.coef,
              model.pred,
              model.stats))
}

# load data
load(paste(filepath, "snt.sorts.RData", sep = ""))
load(paste(filepath, "list.5x5.pf.RData", sep = ""))

# determine fully-fledged model incl. sentiment
full.snt.factors <- snt.sorts %>%
  select(Day, MktRf, SMB, HML, RMW, CMA, PMN)

# run model incl. sentiment
list.full.snt <- run_model(list.5x5.pf[[5]], full.snt.factors)

# extract coefficient for intercept
intercept.snt <- list.full.snt[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(estimate) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE) * 10000

# extract t-statistic for intercept
statistic.intercept <- list.full.snt[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(statistic) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE)

# merge coefficient and intercept
regression_list.snt <- cbind(intercept.snt, NA, statistic.intercept)
rm(intercept.snt,statistic.intercept)

# define factors for fully-fledged model
full.factors <- snt.sorts %>%
  select(Day, MktRf, SMB, HML, RMW, CMA)

# run model without sentiment
list.full <- run_model(list.5x5.pf[[5]], full.factors)

# extract coefficient for intercept
intercept <- list.full[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(estimate) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE) * 10000

# extract t-statistic for intercept
statistic <- list.full[[2]] %>%
  filter(term == "(Intercept)") %>%
  ungroup %>%
  select(statistic) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE)

# merge coefficient and intercept
regression_list <- cbind(intercept, NA, statistic)
rm(intercept, statistic)

# extract coefficient for sentiment
estimate.snt <- list.full.snt[[2]] %>%
  filter(term == "PMN") %>%
  ungroup %>%
  select(estimate) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE) * 10000

# extract t-statistic for sentiment
statistic.snt <- list.full.snt[[2]] %>%
  filter(term == "PMN") %>%
  ungroup %>%
  select(statistic) %>%
  as.matrix %>%
  resize(ncol = 5, nrow = 5, byrow = TRUE)

# merge coefficient and intercept
regression_list.PMN <- cbind(estimate.snt, NA, statistic.snt)
rm(estimate.snt, statistic.snt)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # ####
# Run Fama MacBeth regression ####

require(foreign)
require(plm)
require(lmtest)

load(paste(filepath, "data.snt.d.RData", sep = ""))
load(paste(filepath, "snt.sorts.RData", sep = ""))

##Double-clustering formula (Thompson, 2011)
vcovDC <- function(x, ...){
  vcovHC(x, cluster = "group", ...) + vcovHC(x, cluster = "time", ...) - 
    vcovHC(x, method = "white1", ...)
}

newdata <- snt.sorts %>% 
  merge(data.snt.d, by = "Day") %>%
  mutate(year = format(Day, "%Y"))

save(newdata, file = paste(filepath, "newdata.RData", sep = ""))

fpmg <- pmg(retadj.1mn ~ MktRf + SMB + HML + RMW + CMA + UMD + PMN, newdata, index = c("Day", "PERMNO"))

save(fpmg, file = paste(filepath, "fpmg.RData", sep = ""))

coeftest(fpmg)


