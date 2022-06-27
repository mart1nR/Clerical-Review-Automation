### updated 13 May 2021 to ensure blank CHINumbers are represented as ####
### NA and not '9218868437227407266' - before joining "matches_across_runs" to "incoming_neg" 
### ensure UPI_NUMBER is renamed to CHINumber and chi_pad syntax is run ###

###v2 - update 29/06/2021 - pdated date suffix to "XXXX" and update file paths####
###v3 - update 02/07 - annotated with guidance for creating next loads and saving matched results###

library(odbc)
library(rvest)
library(xml2)
library(ggplot2)
library(lubridate)
library(tidyr)
library(data.table)
library(tidytable)
library(stringdist)
library(stringi)
library(stringr)
library(fuzzyjoin)
library(glue)
library(tidylog)
library(phonics)
library(phsmethods)
library(dplyr)

###################################
paste2 <- function(...,sep=", ") {
  L <- list(...)
  L <- lapply(L,function(x) {x[is.na(x)] <- ""; x})
  gsub(paste0("(^",sep,"|",sep,"$)"),"",
       gsub(paste0(sep,sep),sep,
            do.call(paste,c(L,list(sep=sep)))))
}
######################################

date_to_use <- Sys.time()

user_name <- toupper(Sys.info()['user'])

#datacleaning pattern
pattern <- "XX"#remove XX from names
#WHICH STUDY NUMBER?
STUDY_NO<-4117

#SQL connection to collect pairs file

con <- odbc::dbConnect(drv = odbc::odbc(), dsn = "IDXP",
                       uid = rstudioapi::showPrompt(title = "Username", message = "Username:"),
                       pwd = rstudioapi::askForPassword("INDEXER Password:"), port = "1529",
                       host = "nssindexpdb01.csa.scot.nhs.uk",SVC = "IDXP.nss.scot.nhs.uk")

apexp <- tbl_df(dbGetQuery(con, statement = glue("SELECT INDEXER.ANALYSIS_DETAILS.UPI_NUMBER,
  INDEXER.ANALYSIS_MASTER.SERIAL,
  INDEXER.ANALYSIS_DETAILS.FIRST_FORENAME,
  INDEXER.ANALYSIS_MASTER.FNAME1,
  INDEXER.ANALYSIS_DETAILS.SECOND_FORENAME,
  INDEXER.ANALYSIS_MASTER.FNAME2,
  INDEXER.ANALYSIS_DETAILS.SURNAME,
  INDEXER.ANALYSIS_MASTER.SNAME1,
  INDEXER.ANALYSIS_DETAILS.PREVIOUS_SURNAME,
  INDEXER.ANALYSIS_MASTER.SNAME2,
  INDEXER.ANALYSIS_DETAILS.SEX,
  INDEXER.ANALYSIS_MASTER.SEX AS SEX_IN,
  INDEXER.ANALYSIS_DETAILS.DATE_OF_BIRTH,
  INDEXER.ANALYSIS_MASTER.DOB,
  INDEXER.ANALYSIS_DETAILS.POSTCODE,
  INDEXER.ANALYSIS_MASTER.PCODE,
  INDEXER.ANALYSIS_DETAILS.NHS_NO,
  INDEXER.ANALYSIS_MASTER.NHSNO AS NHS_NO_IN,
  INDEXER.ANALYSIS_DETAILS.MATCH_WEIGHT,
  INDEXER.ANALYSIS_MASTER.STUDY_NO,
  INDEXER.INDEX_SUMMARY.INPUT_RECORDS,
  INDEXER.INDEX_SUMMARY.FILENAME,
  INDEXER.INDEX_SUMMARY.FILE_LENGTH,
  INDEXER.INDEX_SUMMARY.ADDRESS
FROM INDEXER.ANALYSIS_DETAILS
INNER JOIN INDEXER.ANALYSIS_MASTER
ON INDEXER.ANALYSIS_MASTER.SERIAL    = INDEXER.ANALYSIS_DETAILS.SERIAL
AND INDEXER.ANALYSIS_MASTER.STUDY_NO = INDEXER.ANALYSIS_DETAILS.STUDY_NO
INNER JOIN INDEXER.INDEX_SUMMARY
ON INDEXER.INDEX_SUMMARY.STUDY_NO      = INDEXER.ANALYSIS_MASTER.STUDY_NO
WHERE INDEXER.INDEX_SUMMARY.STUDY_NO = {STUDY_NO}")))

Indexer_pairs<-apexp%>%
  filter(MATCH_WEIGHT!=99.999) %>%
  mutate(DOB=ifelse(DOB==0,"NA",DOB)) %>% 
  mutate(FIRST_FORENAME=ifelse(is.na(FIRST_FORENAME),"XXXXXX",FIRST_FORENAME))%>%
  mutate(SECOND_FORENAME=ifelse(is.na(SECOND_FORENAME),"YYYYYY",SECOND_FORENAME))%>%
  mutate(SURNAME=ifelse(is.na(SURNAME),"WWWWWW",SURNAME))%>%
  mutate(PREVIOUS_SURNAME=ifelse(is.na(PREVIOUS_SURNAME),"VVVVVV",PREVIOUS_SURNAME))%>%
  mutate(SNAME1=ifelse(is.na(SNAME1),"QQQQQQ",SNAME1))%>%
  mutate(SNAME2=ifelse(is.na(SNAME2),"KKKKKK",SNAME2))%>%
  mutate(SEX_IN=ifelse(is.na(SEX_IN),9,SEX_IN)) %>% 
  mutate(FN_NA="ZZZZZZ") %>% 
  mutate(SN_NA="QQQQQQ") %>% 
  mutate(FIRST_FORENAME=ifelse(FIRST_FORENAME=="BABY","ZZZZZZ",FIRST_FORENAME)) %>% 
  mutate(FIRST_FORENAME=ifelse(FIRST_FORENAME=="BABYII","ZZZZZZ",FIRST_FORENAME)) %>% 
  group_by(SERIAL)%>%#count of records by incomer to get numbner of rivals count=no of rivals
  mutate(count=unique(n()))%>%
  ungroup() %>% 
  mutate(sname_len=nchar(SNAME1)) %>% 
  ##ONLY FOR sdmd
  mutate(SDMD_SN1=substr(SURNAME,1,1)) %>% 
  mutate(SDMD_SN4=substr(SURNAME,4,4)) %>%
  mutate(SDMD_SN=paste0(SDMD_SN1,SDMD_SN4)) %>% 
  mutate(Fname_len=nchar(FNAME1)) %>% 
  mutate(SDMD_FN1=substr(FIRST_FORENAME,1,1)) %>% 
  mutate(SDMD_FN4=substr(FIRST_FORENAME,4,4)) %>%
  mutate(SDMD_FN=paste0(SDMD_FN1,SDMD_FN4)) %>% 
  mutate(SDMD_FN_DOUBLE=paste0(SDMD_FN1,SDMD_FN1)) %>% 
  
  mutate(FNAME1=str_replace_all(FNAME1, pattern, "")) %>% 
  mutate(SNAME1=str_replace_all(SNAME1, pattern, ""))

# na_count2 <-sapply(Indexer_pairs, function(y) sum(length(which(is.na(y)))))
# na_count3 <- data.frame(na_count2)

# #collect nicknames - SOURCE FUNCTION TO CALL A SECOND WEBSCRAPE SCRIPT
# url <- "https://www.familysearch.org/wiki/en/Traditional_Nicknames_in_Old_Documents_-_A_Wiki_List#A"
# webpage <- read_html(url)
# html_node(webpage,".FCK__ShowTableBorders")%>% 
#   html_nodes("P,p") %>%
#   xml_text() 
# df <- tibble(namer = 
#                xml_find_all(webpage, xpath = "/html/body/div[3]/div[3]/div[4]/div/div/div[1]/table/tbody/tr/td/p/text()") %>% 
#                html_text())
# #clean up
# df <- df %>%
#   mutate(namer = str_replace_all(namer, pattern = " \n", replacement = "")) %>%
#   mutate(namer = str_replace_all(namer, pattern = "-", replacement = "=")) %>%
#   filter(namer != "") %>% 
#   mutate(namer = str_replace_all(namer, pattern = " ", replacement = "")) %>% 
#   separate(namer, into = c("name1", "name2"), sep = "=") %>% 
#   rename(Nicknames=name1,Givennames=name2)
# 
# 
# url <- "https://www.thoughtco.com/matching-up-nicknames-with-given-names-1421939"
# webpage <- read_html(url)
# html_node(webpage,".mntl-sc-block_1-0-9")%>%
#   html_nodes("Td,td") %>%
#   xml_text()
# df1 <- tibble(namer = 
#                 xml_find_all(webpage, xpath = "//*/div/table/tbody/tr/td") %>% 
#                 html_text()) %>% 
#   mutate(ind = rep(c(1, 2),length.out = n())) %>%
#   group_by(ind) %>%
#   mutate(id = row_number()) %>%
#   spread(ind, namer) %>%
#   select(-id) %>% 
#   rename(x=1,y=2) %>% 
#   transform(x = strsplit(x, ",")) %>%
#   unnest(cols=c("x")) %>% 
#   transform(y=strsplit(y,",")) %>% 
#   unnest(cols=c("y")) %>% 
#   distinct() %>% 
#   rename(Nicknames=x,Givennames=y)
# library(readxl)
# familysearch_website <- read_excel("/chi/Martin/fname_equivalent/familysearch_website.xlsx")
# 
# familysearch_website <-familysearch_website %>%
#   mutate(Nicknames = strsplit(as.character(`nick name`), ",")) %>% 
#   unnest(Nicknames) %>% 
#   mutate(Givennames = strsplit(as.character(`proper name`), ",")) %>% 
#   unnest(Givennames) %>%
#   select(Nicknames,Givennames)
# 
# NRS_NICKNAMES <- fread("/chi/Martin/fname_equivalent/NRS_NICKNAMES.csv")
# NRS_NICKNAMES <-NRS_NICKNAMES %>% 
#   rename(Nicknames=Nickname,Givennames=Givenname)
# 
# joined<-rbind(familysearch_website,df1,NRS_NICKNAMES)
# 
# joined<-joined %>%
#   mutate(Nicknames2=toupper(Nicknames)) %>%
#   mutate(Givennames2=toupper(Givennames)) %>% 
#   mutate(Nicknames2=trimws(Nicknames2)) %>% 
#   mutate(Givennames2=trimws(Givennames2)) %>% 
#   distinct()
# 
# 
#   fwrite(joined,"/chi/Martin/joined.txt")
joined<-fread("/chi/Martin/joined.txt")#NICKNAMES FILE FOR NOW AFTER SCRAPING
#POSTCODE CLEANING
pattern <- "\\b(?:([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9]?[A-Za-z]))))\\s?[0-9][A-Za-z]{2}))\\b"
#SPD_2 <- fread("/conf/linkage/output/lookups/Unicode/Geography/Scottish Postcode Directory/Scottish_Postcode_Directory_2020_2.csv")%>%
 
#new postcode directory 29/06/2021
SPD_2 <- fread("/conf/linkage/output/lookups/Unicode/Geography/Scottish Postcode Directory/Scottish_Postcode_Directory_2021_2.csv") %>%
as.data.frame()%>%
  dplyr::select(c(pc7,grid_reference_easting,grid_reference_northing,latitude,longitude,split_indicator,oa2011))
SPD_3 <-distinct(SPD_2,pc7,.keep_all = T)%>%
  mutate(pc7=gsub(" ","",pc7))
Indexer_pairs<-Indexer_pairs %>%
  mutate(POSTCODE=gsub(" ","",POSTCODE))%>% 
  left_join(.,SPD_3,by=c("POSTCODE"="pc7"))
Indexer_pairs<-Indexer_pairs %>%
  mutate(PCODE=gsub(" ","",PCODE))%>%  
  left_join(.,SPD_3,by=c("PCODE"="pc7"))
Indexer_pairs<-Indexer_pairs %>%  
  mutate(PCODE_VALID=grepl(pattern,PCODE))%>%
  mutate(POSTCODE_VALID=grepl(pattern,POSTCODE))%>%
  mutate(longitude.x=ifelse(is.na(longitude.x),99999999,longitude.x))%>%#pcode or postcode log and lat are written to distance points if they are given NA,
  mutate(longitude.y=ifelse(is.na(longitude.y),11111111,longitude.y))%>%
  mutate(latitude.x=ifelse(is.na(latitude.x),11111111,latitude.x))%>%
  mutate(latitude.y=ifelse(is.na(latitude.y),99999999,latitude.y))%>%
  mutate(dist1=sqrt((longitude.x-longitude.y)^2+(latitude.x-latitude.y)^2))%>%
  mutate(DL_PC_SIM = ifelse(PCODE_VALID==TRUE,(stringdist(POSTCODE,PCODE, method =c("dl"),p=0.1)),"QQQQQQ"))%>% 
  mutate(QGRAM_PC_SIM =ifelse(PCODE_VALID==TRUE,(stringdist(POSTCODE,PCODE, method=c("qgram"),useBytes=FALSE,q=3)),"QQQQQQ"))%>%  
  
  #NOT CERTAIN IF REQUIRED
  mutate(QGRAM_FF_SIM = (stringdist(FIRST_FORENAME,FNAME1, method =c("qgram"),useBytes=FALSE,q=2)))%>%
  mutate(QGRAM_FF_SIM =ifelse(is.na(QGRAM_FF_SIM),"25",QGRAM_FF_SIM))%>%
  mutate(QGRAM_SN_SIM = (stringdist(SURNAME,SNAME1, method =c("qgram"),useBytes=FALSE,q=2)))%>%
  mutate(QGRAM_SN_SIM =ifelse(is.na(QGRAM_SN_SIM),"25",QGRAM_SN_SIM))%>%
  mutate(DL_FF_SIM = (stringdist(FIRST_FORENAME,FNAME1, method =c("dl"),p=0.1)))%>%
  mutate(DL_SN_SIM = (stringdist(SURNAME,SNAME1, method =c("dl"),p=0.1)))%>%
  
  #NAME COMPARISONS
  mutate(JW_FF_SIM = ifelse(!is.na(FNAME1),(stringdist(FIRST_FORENAME,FNAME1, method =c("jw"),p=0.2)),999))%>%
  mutate(JW_SN_SIM = ifelse(!is.na(SNAME1),(stringdist(SURNAME,SNAME1, method =c("jw"),p=0.1)),999))%>%
  mutate(JW_combine=(JW_FF_SIM+JW_SN_SIM))%>%
  mutate(JW_FF_SN1_SIM = (stringdist(FIRST_FORENAME,SNAME1, method =c("jw"),p=0.1)))%>%
  mutate(JW_SN_FF_SIM = (stringdist(SURNAME,FNAME1, method =c("jw"),p=0.1)))%>%
  mutate(JW_FF_SN_SIM = (stringdist(FIRST_FORENAME,SNAME1, method =c("jw"),p=0.1)))%>%
  mutate(JW_combine2=(JW_FF_SN1_SIM+JW_SN_FF_SIM))%>%
  
  #dob BY YY MM DD COMPARISONS
  mutate(YOB_IN=substr(DOB,1,4))%>%
  mutate(YOB=substr(DATE_OF_BIRTH,1,4))%>%
  mutate(YEAR_DIFF=as.numeric(YOB)-as.numeric(YOB_IN))%>%
  mutate(MOB_IN=substr(as.numeric(DOB),5,6))%>%
  mutate(MOB=substr(as.numeric(DATE_OF_BIRTH),5,6))%>%
  mutate(MONTH_DIFF=as.numeric(MOB)-as.numeric(MOB_IN))%>%
  mutate(DAY_IN=substr(as.numeric(DOB),7,8))%>%
  mutate(DAY=substr(as.numeric(DATE_OF_BIRTH),7,8))%>%
  mutate(DAY_DIFF=as.numeric(DAY)-as.numeric(DAY_IN))%>%
  
  #initials checks# POTENTIAL TO TURN OFF OR REMOVE BASED ON INCOMING DATA.
  mutate(SN_INITIAL=substr(SURNAME,1,1))%>%
  mutate(PS_INITIAL=substr(PREVIOUS_SURNAME,1,1))%>%
  mutate(FF_INITIAL=substr(FIRST_FORENAME,1,1))%>%
  mutate(SF_INITIAL=substr(SECOND_FORENAME,1,1))%>% 
  mutate(INITIAL_MATCH=ifelse(stringr::str_length(FNAME1)==1&(FF_INITIAL==FNAME1),"MATCH",""))%>% # MATCH FNAME TO ff_name
  mutate(SF_INITIAL_MATCH=ifelse(stringr::str_length(FNAME1)==1&(SF_INITIAL==FNAME1),"MATCH",""))%>% # MATCH fname to Sf_name
  mutate(SN_INITIAL_MATCH=ifelse(stringr::str_length(SNAME1)==1&(SN_INITIAL==SNAME1),"MATCH",""))%>% # MATCH Sname to surname
  mutate(PS_INITIAL_MATCH=ifelse(stringr::str_length(SNAME1)==1&(PS_INITIAL==SNAME1),"MATCH",""))%>%# MATCH Sname to PSurname
  
  #FUSED SURNAMES AT BEGINNING E.G. REIDTHOMPSON - INCOMING REID and vice versa
  mutate(SNAME_LEN_CHECK=ifelse((str_length(SNAME1)-str_length(SURNAME)>3)|(str_length(SURNAME)-str_length(SNAME1)>3),1,0))%>%
  mutate(SURNAME_FUSED_BEGINNING=(substr(SNAME1,1,str_length(SURNAME))))%>%
  mutate(SURNAME_FUSED_END      =(stri_sub(SNAME1,(-(str_length(SURNAME))),-1)))%>%
  mutate(PSURNAME_FUSED_BEGINNING=(substr(PREVIOUS_SURNAME, 1,str_length(SNAME1))))%>%
  mutate(PSURNAME_FUSED_END      =(stri_sub(PREVIOUS_SURNAME,(-(str_length(SNAME1))),-1)))%>%
  mutate(JW_FF_FN_SIM2 = (stringdist(FIRST_FORENAME,FNAME1, method =c("jw"),p=0.1)))%>%
  
  #FORENAME TRANSPOSED WITH SECOND NAME - ASSUMING 1 INCOMING.              
  mutate(JW_SF_FN_SIM2 = (stringdist(SECOND_FORENAME,FNAME1, method =c("jw"),p=0.1)))%>%
  #SNAME TRANSPOSED WITH PREV SURNAME
  mutate(JW_SN_PS_SIM = (stringdist(PREVIOUS_SURNAME,SNAME1, method =c("jw"),p=0.1)))%>%
  #surname fused to previous surname end / begin
  mutate(JW_SN_FBPS_SIM = (stringdist(SURNAME,PSURNAME_FUSED_BEGINNING, method =c("jw"),p=0.1)))%>%
  mutate(JW_SN_FEPS_SIM = (stringdist(SURNAME, PSURNAME_FUSED_END, method =c("jw"),p=0.1)))%>%
  #surname fused to surname beginning and end
  mutate(JW_SN_FBS_SIM = (stringdist(SURNAME,SURNAME_FUSED_BEGINNING, method =c("jw"),p=0.1)))%>%
  mutate(JW_SN_FES_SIM = (stringdist(SURNAME,SURNAME_FUSED_END, method =c("jw"),p=0.1)))%>%
  # combined names similarity
  mutate(JW_combine3=(JW_FF_FN_SIM2+JW_SN_PS_SIM))%>%
  #look at lengths of names check this
  mutate(FNAME_LEN_CHECK=ifelse((str_length(FNAME1)-str_length(FIRST_FORENAME)>3)|(str_length(FIRST_FORENAME)-str_length(FNAME1)>3),1,0))%>%
  #Check forenames to see if they are fused in Chi file # note reverse as well?>naming issue
  mutate(FNAME_FF_FUSED_BEGINNING=(substr(FNAME1, 1,str_length(FIRST_FORENAME))))%>%
  mutate(FNAME_FF_FUSED_END      =(stri_sub(FNAME1,(-(str_length(FIRST_FORENAME))),-1))) %>% 
  mutate(FNAME_SF_FUSED_BEGINNING=(substr(FNAME2, 1,str_length(SECOND_FORENAME))))%>%
  mutate(FNAME_SF_FUSED_END      =(stri_sub(FNAME2,(-(str_length(SECOND_FORENAME))),-1))) %>% 
  #check if forename is surname match
  mutate(JW_FF_SN1_SIM = (stringdist(FIRST_FORENAME,SNAME1, method =c("jw"),p=0.1)))%>%              
  #check if surname and forenames match
  mutate(JW_FN_SN_SIM = (stringdist(SURNAME,FNAME1, method =c("jw"),p=0.1)))%>%
  mutate(JW_FN_PS_SIM = (stringdist(PREVIOUS_SURNAME,FNAME1, method =c("jw"),p=0.1)))%>%
 #check if 2nd forename is fused with incoming forenames end  beginning
  mutate(JW_FN_FBSF_SIM = (stringdist(SECOND_FORENAME,FNAME_SF_FUSED_BEGINNING, method =c("jw"),p=0.1)))%>%
  mutate(JW_FN_FESF_SIM = (stringdist(SECOND_FORENAME, FNAME_SF_FUSED_END, method =c("jw"),p=0.1)))%>%
  #check if 1st forename is fused with incoming forenames end  beginning
  mutate(JW_FN_FBS_SIM = (stringdist(FIRST_FORENAME,FNAME_FF_FUSED_BEGINNING, method =c("jw"),p=0.1)))%>%
  mutate(JW_FN_FES_SIM = (stringdist(FIRST_FORENAME,FNAME_FF_FUSED_END, method =c("jw"),p=0.1)))

###################################################################
# library(ggplot2)
# 
# minhash_lu<-minhash_lu%>%
#   filter(FN_COUNT>5)
#   minhash_lu2<-minhash_lu2%>%
#     filter(FN_COUNT>5)
#   
#   ambiguous<-left_join(minhash_lu,minhash_lu2,by=c("FORENAME"="SURNAME"))
# 
# ambiguous<-ambiguous%>%
#   mutate(reverisble=(log2(FN_COUNT/SN_COUNT)))%>%
#   mutate(rev2=(log2(SN_COUNT/FN_COUNT)))%>%
#   filter(!is.na(rev2))%>%
#   mutate(diff=log2(FN_COUNT-SN_COUNT))%>%
# mutate(test=diff-rev2)
# 
# #mutate(log2=log2(reverisble/rev2))
#   
#   hist(ambiguous$test)
#    
#   
# 
#   filter(!is.na(reverisble))
# ambiguous<-ambiguous%>%
#   arrange(reverisble)%>%
#   filter(minhash_FNAME1==minhash_SNAME1)%>%
#   scatter.smooth(ambiguous$reverisble,ambiguous$FN_COUNT)
###################################################################
# con2 <- dbConnect(odbc(), dsn = "SMRA",
#                   uid = rstudioapi::askForPassword("Database user"),
#                   pwd = rstudioapi::askForPassword("Database password"),
#                   port = "1527",
#                   host = "nssstats01.csa.scot.nhs.uk",
#                   SVC = "SMRA.nss.scot.nhs.uk")
# 
# multi_birth<-  (dbGetQuery(con2, statement = "SELECT L_UPI_DATA.NAMESAKE_CHI,
#                            L_UPI_DATA.UPI_NUMBER,
#                            L_UPI_DATA.NAMESAKE_TYPE
#                            FROM UPIP.L_UPI_DATA L_UPI_DATA
#                            WHERE (L_UPI_DATA.NAMESAKE_CHI IS NOT NULL)
#                            AND ( (L_UPI_DATA.CHI_STATUS = 'C') OR (L_UPI_DATA.CHI_STATUS IS NULL))"))
# 
# # Indexer_pairs <-apexp %>%
# #   mutate(UPI_NUMBER=as.character(UPI_NUMBER)) %>%
# #   mutate(UPI_NUMBER=phsmethods::chi_pad(UPI_NUMBER)) %>%
# #   mutate(SERIAL=as.character(SERIAL))
# library(tidylog)
# 
# Indexer_pairs_twin <-left_join(apexp,multi_birth,by="UPI_NUMBER")
# Indexer_pairs_twin <-Indexer_pairs_twin %>%
#   filter(!is.na(NAMESAKE_CHI)) %>%
#  
# 
#   select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,NAMESAKE_CHI,NAMESAKE_TYPE,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,SURNAME,SNAME1,DATE_OF_BIRTH,DOB,SEX,SEX_IN)

Indexer_pairs1<-Indexer_pairs%>%
  filter(count==1)# PICK ONLY UNRIVALLED MATCHES TO CHECK FIRST.

stage1<-Indexer_pairs1 %>% 
  #precise join on sex,strict jac_similarity across both names,dateofbirth match allowing day month transpositions, very similar postcode
  filter((SEX==SEX_IN)&(JW_combine<0.165)&(DATE_OF_BIRTH==DOB|(YOB==YOB_IN&MOB==DAY_IN&DAY==MOB_IN)&(PCODE==POSTCODE|DL_PC_SIM>5)))%>%# match key pattern
  select(SERIAL,MATCH_WEIGHT,JW_FF_SIM,JW_SN_SIM,UPI_NUMBER,PCODE,POSTCODE,FIRST_FORENAME,FNAME1,SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM) 

one<-stage1
matches1<-one 
one<-one %>% 
  mutate(stage=1) %>% 
  select(SERIAL,UPI_NUMBER,stage)

stage2<-anti_join(Indexer_pairs1,one,by="SERIAL")
stage2<-stage2 %>%  #as above but name transpositions    
  filter((SEX==SEX_IN)&(JW_combine2<"0.195")&(DATE_OF_BIRTH==DOB|(YOB==YOB_IN&MOB==DAY_IN&DAY==MOB_IN)&(PCODE==POSTCODE|DL_PC_SIM>5)))%>%
  #precise join on sex,jw_similarity,dateofbirth slightly more name differences but names transposed
  select(SERIAL,UPI_NUMBER,PCODE,POSTCODE,FIRST_FORENAME,FNAME1,SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM) 
two<-stage2%>%
  mutate(stage=2)%>% 
  select(SERIAL,UPI_NUMBER,stage)
matches2<-rbind(one,two)

stage3<-anti_join(Indexer_pairs1,matches2,by="SERIAL")
stage3<-stage3 %>% #gender agrees close name matches, dob differs in a controlled manner
  filter((SEX==SEX_IN)
         &(JW_combine<"0.16")
         &(YEAR_DIFF==1|YEAR_DIFF==-1|YEAR_DIFF==2|YEAR_DIFF==-2|YEAR_DIFF==3|YEAR_DIFF==-3|YEAR_DIFF==10|YEAR_DIFF==-10|YEAR_DIFF==-10|YEAR_DIFF==0)
         &(MONTH_DIFF==1|MONTH_DIFF==-1|MONTH_DIFF==0)
         &(DAY_DIFF==1|DAY_DIFF==-1|DAY_DIFF==2|DAY_DIFF==-2|DAY_DIFF==3
           |DAY_DIFF==-3|DAY_DIFF==4|DAY_DIFF==-4|DAY_DIFF==5|DAY_DIFF==-5
           |DAY_DIFF==6|DAY_DIFF==-6|DAY_DIFF==10|DAY_DIFF==-10
           |DAY_DIFF==0)&(PCODE==POSTCODE|DL_PC_SIM>5))%>%
  
  select(YOB,YOB_IN,MOB,MOB_IN,DAY,DAY_IN,SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,FNAME1,SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM)
three<-stage3%>%
  mutate(stage=3)%>% 
  select(SERIAL,UPI_NUMBER,stage)
matches3<-rbind(one,two,three)

stage4<-anti_join(Indexer_pairs1,matches3,by="SERIAL")#BOTH NAMES ON 1 CHARACTER LONG OR INITAL ONLY FOR FORENAME AND GOOD SNAME MATCH.
stage4<-stage4 %>%   
  filter(((stringr::str_length(FNAME1)==1)&(stringr::str_length(SNAME1)==1))|((stringr::str_length(FNAME1)==1)&(stringr::str_length(SNAME1)>1)))%>%
  #where initials only hard match close match on surname and pcode where they exist and controlled variation in dob
  filter(((PCODE==POSTCODE)&(INITIAL_MATCH=="MATCH")
          &(SEX_IN==SEX)&(DOB==DATE_OF_BIRTH))
         |(JW_SN_SIM<"0.1"&INITIAL_MATCH=="MATCH")
         &(SEX_IN==SEX)
         &(YEAR_DIFF==1|YEAR_DIFF==-1|YEAR_DIFF==2|YEAR_DIFF==-2|YEAR_DIFF==3|YEAR_DIFF==-3|YEAR_DIFF==10|YEAR_DIFF==-10|YEAR_DIFF==-10|YEAR_DIFF==0)
         &(MONTH_DIFF==1|MONTH_DIFF==-1|MONTH_DIFF==0)&(DAY_DIFF==1|DAY_DIFF==-1|DAY_DIFF==2|DAY_DIFF==-2|DAY_DIFF==3|DAY_DIFF==-3|DAY_DIFF==4|DAY_DIFF==-4|DAY_DIFF==5|DAY_DIFF==-5|DAY_DIFF==6|DAY_DIFF==-6|DAY_DIFF==10|DAY_DIFF==-10|DAY_DIFF==0))%>%
  select(SERIAL,UPI_NUMBER,PCODE,POSTCODE,FIRST_FORENAME,FNAME1,SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM)

four<-stage4%>% 
  mutate(stage=4) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches4<-rbind(one,two,three,four)

stage5<-anti_join(Indexer_pairs1,matches4,by="SERIAL")
#NO NAMESs at all relie on entered nhsno and pcode and dob and sex being correct. OR NO CHI OR NAMES BUT matches where sex, dob & pcode result in only one unique candidate match
stage5<-stage5 %>% 
  filter((is.na(FNAME1)&is.na(SNAME1))&
           ((SEX==SEX_IN)&(PCODE==POSTCODE)&(DOB==DATE_OF_BIRTH))&(NHS_NO==NHS_NO_IN))%>%
  select(MATCH_WEIGHT,SERIAL,UPI_NUMBER,PCODE,POSTCODE,FIRST_FORENAME,FNAME1,SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM)  
#stage5_matches<-rbind(stage5_matches1,stage5_matches2)
five<-stage5%>%   
  mutate(stage=5) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches5<-rbind(one,two,three,four,five)

stage6<-anti_join(Indexer_pairs1,matches5,by="SERIAL")
stage6<- stage6 %>% 
  # NAME TRANPOSITIONS MINOR DIFFERENCES
  filter((FNAME1!="ZZZZZZ")&(SNAME1!="QQQQQQ")) %>% filter(((JW_SN_SIM<"0.14" & JW_FF_SIM<"0.15")
                                                            |(JW_SN_SIM<"0.14" & JW_FF_FN_SIM2<"0.15")
                                                            |(JW_SN_FEPS_SIM<"0.1" & JW_FF_SIM<"0.15")
                                                            |(JW_SN_FF_SIM<"0.14" & JW_FF_SN1_SIM<"0.15")
                                                            |(JW_SN_FEPS_SIM<"0.1"&  JW_FF_FN_SIM2<"0.15")
                                                            |(JW_SF_FN_SIM2<"0.14" & JW_SN_SIM<"0.15")
                                                            |(JW_SF_FN_SIM2<"0.14" & JW_SN_PS_SIM<"0.15")
                                                            |(JW_SF_FN_SIM2<"0.14" & JW_SN_FEPS_SIM<"0.1")
                                                            |(JW_FN_SN_SIM<"0.14" & JW_SN_PS_SIM <"0.15")#
                                                            |(JW_FN_SN_SIM<"0.14" & JW_SN_FBPS_SIM<"0.1")#
                                                            |(JW_FN_SN_SIM<"0.14" & JW_SN_FEPS_SIM<"0.1")#
                                                            |(JW_FN_SN_SIM<"0.14" & JW_SN_FBS_SIM<"0.1")#
                                                            |(JW_FN_SN_SIM<"0.14" & JW_SN_FES_SIM<"0.1")
                                                            |(JW_FN_FBSF_SIM<"0.1" & JW_SN_PS_SIM <"0.15")
                                                            |(JW_FN_FBSF_SIM<"0.1" & JW_SN_FBPS_SIM<"0.1")
                                                            |(JW_FN_FBSF_SIM<"0.1" & JW_SN_FEPS_SIM<"0.1")
                                                            |(JW_FN_FBSF_SIM<"0.1" & JW_SN_FBS_SIM<"0.15")
                                                            |(JW_FN_FBSF_SIM<"0.1" & JW_SN_FES_SIM<"0.15")
                                                            |(JW_FN_FESF_SIM<"0.1" & JW_SN_PS_SIM <"0.15")
                                                            |(JW_FN_FESF_SIM<"0.1" & JW_SN_FBPS_SIM<"0.1")
                                                            |(JW_FN_FESF_SIM<"0.1" & JW_SN_FEPS_SIM<"0.1")
                                                            |(JW_FN_FESF_SIM<"0.1" & JW_SN_FBS_SIM<"0.1")
                                                            |(JW_FN_FESF_SIM<"0.1" & JW_SN_FES_SIM<"0.11")
                                                            |(JW_FN_FBS_SIM<"0.1" & JW_SN_PS_SIM <"0.15")
                                                            |(JW_FN_FBS_SIM<"0.1" & JW_SN_FBPS_SIM<"0.1")
                                                            |(JW_FN_FBS_SIM<"0.1" & JW_SN_FEPS_SIM<"0.1")
                                                            |(JW_FN_FBS_SIM<"0.1" & JW_SN_FBS_SIM<"0.1")
                                                            |(JW_FN_FBS_SIM<"0.1" & JW_SN_FES_SIM<"0.1")
                                                            |(JW_FN_FES_SIM<"0.1" & JW_SN_PS_SIM <"0.14")
                                                            |(JW_FN_FES_SIM<"0.1" & JW_SN_FBPS_SIM<"0.1")
                                                            |(JW_FN_FES_SIM<"0.1" & JW_SN_FEPS_SIM<"0.1")
                                                            |(JW_FN_FES_SIM<"0.1" & JW_SN_FBS_SIM<"0.1")
                                                            |(JW_FN_FES_SIM<"0.1" & JW_SN_FES_SIM<"0.1")
                                                            |(JW_FF_FN_SIM2<"0.14" & JW_SN_PS_SIM <"0.14")
                                                            |(JW_FF_FN_SIM2<"0.14" & JW_SN_FBPS_SIM<"0.1")
                                                            |(JW_FF_FN_SIM2<"0.14" & JW_SN_FEPS_SIM<"0.1")
                                                            |(JW_FF_FN_SIM2<"0.14" & JW_SN_FBS_SIM<"0.1")
                                                            |(JW_FF_FN_SIM2<"0.14" & JW_SN_FES_SIM<"0.1")
                                                            |(JW_SF_FN_SIM2<"0.14" & JW_SN_PS_SIM <"0.14")
                                                            |(JW_SF_FN_SIM2<"0.14" & JW_SN_FBPS_SIM<"0.1")
                                                            |(JW_SF_FN_SIM2<"0.14" & JW_SN_FEPS_SIM<"0.1")
                                                            |(JW_SF_FN_SIM2<"0.14" & JW_SN_FBS_SIM<"0.14")
                                                            |(JW_SF_FN_SIM2<"0.14" & JW_SN_FES_SIM<"0.1")
                                                            |(JW_FN_SN_SIM<"0.14" & JW_SN_FF_SIM<"0.14"))
                                                           & (((SEX_IN==SEX)& ((between(YEAR_DIFF,-3,3))
                                                                 &between(MONTH_DIFF,-1,1)
                                                                 &between(DAY_DIFF,-5,5)))
                                                             |((YOB==YOB_IN) & (MOB==DAY_IN) & (DAY==MOB_IN))
                                                             |((DOB==DATE_OF_BIRTH))))%>%
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,JW_FF_FN_SIM2,JW_SN_FBPS_SIM,SURNAME_FUSED_BEGINNING,JW_SN_FEPS_SIM,SURNAME_FUSED_END,JW_SN_PS_SIM,JW_combine3,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM)
six<- stage6%>% 
  mutate(stage=6) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches6<-rbind(one, two,three,four,five,six)

stage7<-anti_join(Indexer_pairs1,matches6,by="SERIAL")
stage7<-stage7 %>% 
  #gender mis match ALLOWED STRICT otherwise.
  filter(((JW_SN_SIM<"0.15"&JW_FF_SIM<"0.15")
          |(JW_SN_SIM<"0.15"&JW_FF_FN_SIM2<"0.15")
          |(JW_SN_FEPS_SIM<"0.15"&JW_FF_SIM<"0.15")
          |(JW_SN_FF_SIM<"0.15"&JW_FF_SN1_SIM<"0.15")
          |(JW_SN_FEPS_SIM<"0.15"&JW_FF_FN_SIM2<"0.15"))
         &(as.numeric(SEX_IN)!=as.numeric(SEX)|(SEX_IN==SEX))
         &(PCODE==POSTCODE)) %>% 
    select(SERIAL,UPI_NUMBER,PCODE,POSTCODE,FIRST_FORENAME,FNAME1,PREVIOUS_SURNAME,SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM)

seven<-stage7%>%
  mutate(stage=7) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches7<-rbind(one,two,three,four,five,six,seven)

stage8<-anti_join(Indexer_pairs1,matches7,by="SERIAL")
stage8<-stage8 %>% 
  filter(((DL_PC_SIM<=2)&(!is.na(FNAME1)&!is.na(SNAME1)&JW_SN_SIM<=0.3&(JW_FF_SIM<=0.19&SEX_IN==SEX))&(MATCH_WEIGHT>=25))
    |(DL_PC_SIM<=2&(!is.na(FNAME1)&!is.na(SNAME1)&JW_SN_SIM<=0.19)
       &(JW_FF_SIM<=0.3&SEX_IN==SEX)&(MATCH_WEIGHT>=25))) %>% 
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM)
eight<-stage8%>%
  mutate(stage=8) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches8<-rbind(one,two,three,four,five,six,seven,eight)

stage9<-anti_join(Indexer_pairs1,matches8,by="SERIAL")
stage9<-stage9 %>% 
  filter(JW_FF_FN_SIM2<=0.15 & JW_SN_SIM<=0.15 & DOB==DATE_OF_BIRTH & DL_PC_SIM<=4) %>% 
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM)

nine<-stage9 %>%
  mutate(stage=9) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches9<-rbind(one, two,three,four,five,six,seven,eight,nine)

stage10<-anti_join(Indexer_pairs1,matches9,by="SERIAL")
stage10<-stage10 %>% 
  filter((PCODE==POSTCODE&(FNAME1==FN_NA&SNAME1==SURNAME)&SEX_IN==SEX&DOB==DATE_OF_BIRTH)
         |(PCODE==POSTCODE&(FNAME1==FIRST_FORENAME&SNAME1==SN_NA)&SEX_IN==SEX&DOB==DATE_OF_BIRTH))

ten<-stage10 %>%
  mutate(stage=10) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches10<-rbind(one, two,three,four,five,six,seven,eight,nine,ten)

stage11<-anti_join(Indexer_pairs1,matches10,by="SERIAL")
stage11<-stage11 %>% 
  mutate(SSL_fname1=str_length(FNAME1)) %>% 
  mutate(SSL_sname1=str_length(SNAME1)) %>% 
  filter((SSL_fname1<="2") & (SSL_sname1<="2")) %>% 
  filter(INITIAL_MATCH=="MATCH" & SN_INITIAL_MATCH=="MATCH"
         |INITIAL_MATCH=="MATCH" & PS_INITIAL_MATCH=="MATCH" 
         |SF_INITIAL_MATCH=="MATCH" & SN_INITIAL_MATCH=="MATCH"
         |SF_INITIAL_MATCH=="MATCH" & PS_INITIAL_MATCH=="MATCH" 
  ) %>% 
  filter(YEAR_DIFF==0&MONTH_DIFF==0
         |YEAR_DIFF==0&DAY_DIFF==0
         |MONTH_DIFF==0&DAY_DIFF==0)%>%
  filter(DL_PC_SIM<=4)%>%
  
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM)

eleven<-stage11 %>% 
  mutate(stage=11) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches11<-rbind(one, two,three,four,five,six,seven,eight,nine,ten, eleven)

stage12<-anti_join(Indexer_pairs1,matches11,by="SERIAL")
stage12<- stage12 %>% 
  
  filter((JW_FF_SIM<=0.15 & JW_SN_SIM<=0.15)
         |(JW_SN_PS_SIM<=0.15 & JW_SF_FN_SIM2<=0.15
           |JW_FF_SIM<=0.15 &JW_SN_PS_SIM<=0.15
           |JW_SF_FN_SIM2<=0.15&JW_SN_SIM<=0.15)
         |(JW_SN_SIM<=0.15 & JW_FF_FN_SIM2<=0.15)
         |(JW_SN_FEPS_SIM<=0.15 & JW_FF_SIM<=0.15)
         |(JW_SN_FF_SIM<=0.15 & JW_FF_SN1_SIM<=0.15)
         |(JW_SN_FEPS_SIM<=0.15 & JW_FF_FN_SIM2<=0.15))%>%
  filter(YEAR_DIFF==0&MONTH_DIFF==0
         |YEAR_DIFF==0&DAY_DIFF==0
         |MONTH_DIFF==0&DAY_DIFF==0&between(YEAR_DIFF,-10,10))%>%
  filter(DL_PC_SIM<=4)%>%
  
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM)

twelve<-stage12 %>%
  mutate(stage=12) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches12<-rbind(one, two,three,four,five,six,seven,eight,nine,ten, eleven,twelve) 

#ADD CHI MATCH, Fname nicknames,SNAME, DOB, PCODE
stage13<-anti_join(Indexer_pairs1,matches12,by="SERIAL")

stage13<-left_join(stage13,joined,by=c("FNAME1"="Nicknames","FIRST_FORENAME"="Givennames"))

stage13<-stage13 %>% 
  distinct() %>% 
  ##catalogue forename similarity assuming it is an nickname
  
  filter((!is.na(Givennames2)&!is.na(Nicknames2))&(SNAME1==SURNAME|SNAME1==PREVIOUS_SURNAME|JW_SN_SIM<=0.1|JW_SN_PS_SIM<=0.1)
         &(DOB==DATE_OF_BIRTH&(YEAR_DIFF==0|MONTH_DIFF==0)|(MONTH_DIFF==0|DAY_DIFF==0&between(YEAR_DIFF,-10,10))
           |(YOB==YOB_IN&MOB==DAY_IN&DAY==MOB_IN)  |(DAY_DIFF==0|YEAR_DIFF==0))#limit yob range and add tranpositions
         &(SEX==SEX_IN)) %>% 
  
  select(SERIAL,Nicknames2,Givennames2,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) 

thirteen<-stage13 %>% 
  mutate(stage=13) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches13<-rbind(one, two,three,four,five,six,seven,eight,nine,ten, eleven,twelve,thirteen) 


############################
######## RIVALS ############
############################

Indexer_pairs2<-Indexer_pairs%>%
  filter(MATCH_WEIGHT!=99) %>% 
  filter(count>=2)

reformed_s1<-Indexer_pairs2 
reformed_s1<-reformed_s1 %>%   
  filter(!is.na(FNAME1)&!is.na(SNAME1)) %>%
  filter((JW_FF_SIM<=0.1 & JW_SN_SIM<=0.1 & SEX==SEX_IN & DL_PC_SIM<3)
         &((YEAR_DIFF==0&MONTH_DIFF==0)|(YEAR_DIFF==0&DAY_DIFF==0)|(DAY_DIFF==0&MONTH_DIFF==0&between(YEAR_DIFF,-9,9))
           |(YOB==YOB_IN&MOB==DAY_IN&DAY==MOB_IN)) )%>% 
  
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
  
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>% 
  filter(count_rivals==1) %>% 
  ungroup()#get rid of records where diff can't be decided.
oner<-reformed_s1 %>% 
  mutate(stage=14) %>% 
  select(SERIAL,UPI_NUMBER,stage)

#Rivals stage 2 transposed names and good.
reformed_r2<-anti_join(Indexer_pairs2,reformed_s1,by="SERIAL")

reformed_s2<-reformed_r2 %>% 
  filter(!is.na(FIRST_FORENAME)&!is.na(SURNAME)&!is.na(DATE_OF_BIRTH)) %>% 
  filter(( FNAME1==SURNAME&SNAME1==FIRST_FORENAME
           |JW_FF_SN1_SIM<=0.2& JW_FN_SN_SIM<=0.15
           |JW_SF_FN_SIM2<=0.2& JW_SN_PS_SIM<=0.15
           |JW_SN_FBS_SIM<=0.15& JW_FF_SIM<=0.15
           |JW_SN_FES_SIM<=0.15& JW_FF_SIM<=0.15)
  &(SEX==SEX_IN)
  &(YEAR_DIFF==0&MONTH_DIFF==0|YEAR_DIFF==0&DAY_DIFF==0|DAY_DIFF==0&MONTH_DIFF==0&between(YEAR_DIFF,-5,5))
  &(DL_PC_SIM<=2)) %>% 
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,JW_FF_SN1_SIM,JW_FN_SN_SIM,JW_SF_FN_SIM2,JW_SN_PS_SIM,JW_SN_FBS_SIM,JW_FF_SIM,JW_SN_FES_SIM,JW_FF_SIM,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
  
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>% 
  filter(count_rivals==1) %>% 
  ungroup() 
twor<-reformed_s2 %>% 
  mutate(stage=15) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches2r<-rbind(oner,twor) 

rivals3<-anti_join(Indexer_pairs2,matches2r,by=("SERIAL"))

rivals3<-rivals3 %>% 
  arrange(SERIAL) %>% 
  filter((NHS_NO==NHS_NO_IN)) %>%  
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>% 
  filter(count_rivals==1) %>% 
  ungroup() 

threer<-rivals3 %>% 
  mutate(stage=16) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches3r<-rbind(oner,twor,threer) 

rivals4<-anti_join(Indexer_pairs2,matches3r,by=("SERIAL"))

rivals4<-rivals4 %>%
  
  #PRECISE MATCH ON GENDER , DOB, AND NAMES BUT NAMES FUSED OR TRANSPOSED
  arrange(SERIAL) %>%
  filter((FNAME1==FIRST_FORENAME&SNAME1==SURNAME
          |FNAME1==FIRST_FORENAME&SURNAME==SURNAME_FUSED_BEGINNING
          |FNAME1==FIRST_FORENAME&SURNAME==SURNAME_FUSED_END
          |FNAME1==FIRST_FORENAME&SNAME1==PREVIOUS_SURNAME
          |FNAME1==SURNAME&SNAME1==FIRST_FORENAME
          |FNAME1==SECOND_FORENAME&SNAME1==PREVIOUS_SURNAME
          |FNAME1==SECOND_FORENAME&SNAME1==SURNAME
          |FNAME1==PREVIOUS_SURNAME&SNAME1==SECOND_FORENAME) 
  &(SEX_IN==SEX)&(DOB==DATE_OF_BIRTH) &(PCODE==POSTCODE|DL_PC_SIM>=4)) %>% 
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>% 
  filter(count_rivals==1) %>% 
  ungroup()
fourr<-rivals4 %>% 
  mutate(stage=17) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches4r<-rbind(oner,twor,threer,fourr) 

rivals5<-anti_join(Indexer_pairs2,matches4r,by=("SERIAL"))

rivals5<-rivals5 %>%#jw TOLERANCE ON NAMES but TIGHT TOLERANCE ON DOB AND ON GENDER BUT ONLY IF DOB IS CORRECT.1 OR 2 CHAR TOLERANCE ON PC 
  filter((FNAME1!="ZZZZZZ")&(SNAME1!="QQQQQQ")) %>%
  filter((JW_FF_SIM<=0.1&JW_SN_SIM<=0.1
          |FNAME_FF_FUSED_BEGINNING<=0&JW_SN_SIM<=0.1
          |FNAME_FF_FUSED_END<=0&JW_SN_SIM<=0.1
          |FNAME_SF_FUSED_BEGINNING<=0&JW_SN_SIM<=0.1
          |FNAME_SF_FUSED_END<=0&JW_SN_SIM<=0.1
          |JW_FF_SN1_SIM<=0.1&JW_FN_SN_SIM<=0.1
          |JW_FN_PS_SIM<=0.1&JW_FF_SN1_SIM<=0.1
          |JW_FN_FBSF_SIM<=0&JW_SN_SIM<=0.1
          |JW_FN_FESF_SIM<=0&JW_SN_SIM<=0.1
          |JW_FN_FBS_SIM<=0&JW_SN_SIM<=0.1
          |JW_FN_FES_SIM<=0&JW_SN_SIM<=0.1) 
         &((DOB==DATE_OF_BIRTH)|((SEX_IN==SEX) 
              &(YEAR_DIFF==0|YEAR_DIFF==1|YEAR_DIFF==-1|YEAR_DIFF==10|YEAR_DIFF==-10
                |YEAR_DIFF==2|YEAR_DIFF==-2|YEAR_DIFF==3|YEAR_DIFF==-3|YEAR_DIFF==4|YEAR_DIFF==-4
                |YEAR_DIFF==5|YEAR_DIFF==-5)
              &(MONTH_DIFF==0|MONTH_DIFF==1|MONTH_DIFF==-1|MONTH_DIFF==10|MONTH_DIFF==-10|
                   MONTH_DIFF==2|MONTH_DIFF==-2|MONTH_DIFF==3|MONTH_DIFF==-3)
              &(DAY_DIFF==0|DAY_DIFF==1|DAY_DIFF==-1|DAY_DIFF==10|DAY_DIFF==-10|DAY_DIFF==2|DAY_DIFF==-2|DAY_DIFF==3|DAY_DIFF==-3)))
         &(PCODE==POSTCODE|DL_PC_SIM<3))%>% 
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) 

rivals5<-rivals5 %>%
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>% 
  filter(count_rivals==1)%>% 
  ungroup()

fiver<-rivals5 %>% 
  mutate(stage=18) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches5r<-rbind(oner,twor,threer,fourr,fiver) 

rivals6<-anti_join(Indexer_pairs2,matches5r,by=("SERIAL"))
rivals6<-rivals6 %>% 
  filter(SNAME1!="QQQQQQ"&FNAME1!="ZZZZZZ") %>% 
  filter(FNAME1==FIRST_FORENAME&SNAME1==SURNAME&DOB==DATE_OF_BIRTH&SEX_IN==SEX) %>% 
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>% 
  filter(count_rivals==1)%>% 
  ungroup()

sixr<-rivals6 %>% 
  mutate(stage=19) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches6r<-rbind(oner,twor,threer,fourr,fiver,sixr)

rivals7<-anti_join(Indexer_pairs2,matches6r,by=("SERIAL"))
rivals7<-rivals7 %>% 
  filter((FNAME1==SURNAME&SNAME1==FIRST_FORENAME|JW_FF_SN1_SIM<0.15 &JW_FN_SN_SIM<0.15)
         &((DOB==DATE_OF_BIRTH)
           |((SEX_IN==SEX) &(YEAR_DIFF==0|YEAR_DIFF==1|YEAR_DIFF==-1|YEAR_DIFF==10|YEAR_DIFF==-10
                             |YEAR_DIFF==2|YEAR_DIFF==-2|YEAR_DIFF==3|YEAR_DIFF==-3|YEAR_DIFF==4|YEAR_DIFF==-4
                             |YEAR_DIFF==5|YEAR_DIFF==-5)
             &(MONTH_DIFF==0|MONTH_DIFF==1|MONTH_DIFF==-1|MONTH_DIFF==10|MONTH_DIFF==-10|MONTH_DIFF==11
                |MONTH_DIFF==-11|MONTH_DIFF==2|MONTH_DIFF==-2|MONTH_DIFF==3|MONTH_DIFF==-3|MONTH_DIFF==4
                |MONTH_DIFF==-4|MONTH_DIFF==5|MONTH_DIFF==-5|MONTH_DIFF==6|MONTH_DIFF==-6|MONTH_DIFF==7
                |MONTH_DIFF==-7|MONTH_DIFF==8|MONTH_DIFF==-8|MONTH_DIFF==9|MONTH_DIFF==-9)
             &(DAY_DIFF==0|DAY_DIFF==1|DAY_DIFF==-1|DAY_DIFF==10|DAY_DIFF==-10|DAY_DIFF==18
                |DAY_DIFF==-18|DAY_DIFF==9|DAY_DIFF==-9|DAY_DIFF==2|DAY_DIFF==-2|DAY_DIFF==3|DAY_DIFF==-3
                |DAY_DIFF==4|DAY_DIFF==-4|DAY_DIFF==5|DAY_DIFF==-5)))&(PCODE==POSTCODE|DL_PC_SIM==1|DL_PC_SIM==2)) %>% 
  select(JW_FF_SN1_SIM,JW_FN_SN_SIM,SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>% 
  filter(count_rivals==1)%>% 
  ungroup()
sevenr<-rivals7 %>% 
  mutate(stage=20) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches7r<-rbind(oner,twor,threer,fourr,fiver,sixr,sevenr)

rivals8<-anti_join(Indexer_pairs2,matches7r,by=("SERIAL"))
rivals8<-rivals8 %>%
  filter((SNAME1==SURNAME & FNAME1==FIRST_FORENAME
          |SNAME1==SURNAME & FNAME1==SECOND_FORENAME
          |SNAME1==PREVIOUS_SURNAME & FNAME1==SECOND_FORENAME
          |SNAME1==PREVIOUS_SURNAME & FNAME1==FIRST_FORENAME
          |SNAME1==FIRST_FORENAME & FNAME1 == SURNAME
          |SNAME1==SECOND_FORENAME & FNAME1 == SURNAME
          |SNAME1==FIRST_FORENAME & FNAME1 == PREVIOUS_SURNAME
          |SNAME1==SECOND_FORENAME & FNAME1 == PREVIOUS_SURNAME)
         &((DOB==DATE_OF_BIRTH)
           |(YEAR_DIFF==0|YEAR_DIFF==1|YEAR_DIFF==-1|YEAR_DIFF==10|YEAR_DIFF==-10
             |YEAR_DIFF==2|YEAR_DIFF==-2|YEAR_DIFF==3|YEAR_DIFF==-3|YEAR_DIFF==4|YEAR_DIFF==-4
             |YEAR_DIFF==5|YEAR_DIFF==-5))
         & (MONTH_DIFF==0|MONTH_DIFF==1|MONTH_DIFF==-1|MONTH_DIFF==10|MONTH_DIFF==-10|MONTH_DIFF==11
            |MONTH_DIFF==-11|MONTH_DIFF==2|MONTH_DIFF==-2|MONTH_DIFF==3|MONTH_DIFF==-3|MONTH_DIFF==4
            |MONTH_DIFF==-4|MONTH_DIFF==5|MONTH_DIFF==-5|MONTH_DIFF==6|MONTH_DIFF==-6|MONTH_DIFF==7
            |MONTH_DIFF==-7|MONTH_DIFF==8|MONTH_DIFF==-8|MONTH_DIFF==9|MONTH_DIFF==-9)
         &(DAY_DIFF==0|DAY_DIFF==1|DAY_DIFF==-1|DAY_DIFF==10|DAY_DIFF==-10|DAY_DIFF==18
            |DAY_DIFF==-18|DAY_DIFF==9|DAY_DIFF==-9|DAY_DIFF==2|DAY_DIFF==-2|DAY_DIFF==3|DAY_DIFF==-3
            |DAY_DIFF==4|DAY_DIFF==-4|DAY_DIFF==5|DAY_DIFF==-5)
         &(SEX_IN==SEX|SEX_IN!=SEX)
         &((PCODE==POSTCODE)|(DL_PC_SIM==1|DL_PC_SIM==2))) %>% 
  select(JW_FF_SN1_SIM,JW_FN_SN_SIM,SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>% 
  filter(count_rivals==1)%>% 
  ungroup()
eightr<-rivals8 %>% 
  mutate(stage=21) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches8r<-rbind(oner,twor,threer,fourr,fiver,sixr,sevenr,eightr)

rivals9<-anti_join(Indexer_pairs2,matches8r,by=("SERIAL"))
rivals9<-rivals9 %>%
  filter(FNAME1!=FN_NA|SNAME1!=SN_NA) %>% 
  filter ((SNAME1==SURNAME&JW_FF_SIM<=0.1
           |FNAME1==FIRST_FORENAME&JW_SN_SIM<=0.1
           |SNAME1==PREVIOUS_SURNAME&JW_FF_SIM<=0.1
           |FNAME1==SECOND_FORENAME&JW_SN_SIM<=0.1
           |SNAME1==SECOND_FORENAME & FNAME1==PREVIOUS_SURNAME)
          &((DOB==DATE_OF_BIRTH)
            |(YEAR_DIFF==0|YEAR_DIFF==1|YEAR_DIFF==-1|YEAR_DIFF==10|YEAR_DIFF==-10)
            &(MONTH_DIFF==0|MONTH_DIFF==1|MONTH_DIFF==-1)
            &(DAY_DIFF==0|DAY_DIFF==1|DAY_DIFF==-1|DAY_DIFF==10|DAY_DIFF==-10|DAY_DIFF==18
               |DAY_DIFF==-18|DAY_DIFF==9|DAY_DIFF==-9))
          &(SEX_IN==SEX)
          &((PCODE==POSTCODE)|(DL_PC_SIM==1|DL_PC_SIM==2))) %>% 
  select(JW_FF_SN1_SIM,JW_FN_SN_SIM,FN_NA,SN_NA,SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>% 
  filter(count_rivals==1)%>% 
  ungroup()

niner<-rivals9 %>% 
  mutate(stage=22) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches9r<-rbind(oner,twor,threer,fourr,fiver,sixr,sevenr,eightr,niner)

rivals10<-anti_join(Indexer_pairs2,matches9r,by=("SERIAL"))
rivals10<-rivals10 %>%
  filter(nchar(rivals10$FNAME1)==1 &nchar(rivals10$SNAME1)&
           (INITIAL_MATCH == "MATCH"&SN_INITIAL_MATCH== "MATCH"
            |SF_INITIAL_MATCH== "MATCH"&PS_INITIAL_MATCH== "MATCH"
            |INITIAL_MATCH == "MATCH"&PS_INITIAL_MATCH== "MATCH"
            |SF_INITIAL_MATCH== "MATCH"&SN_INITIAL_MATCH== "MATCH")
         &((DOB==DATE_OF_BIRTH)
           |(YEAR_DIFF==0|YEAR_DIFF==1|YEAR_DIFF==-1|YEAR_DIFF==10|YEAR_DIFF==-10)
           &(MONTH_DIFF==0|MONTH_DIFF==1|MONTH_DIFF==-1)
           &(DAY_DIFF==0|DAY_DIFF==1|DAY_DIFF==-1|DAY_DIFF==10|DAY_DIFF==-10|DAY_DIFF==18
              |DAY_DIFF==-18|DAY_DIFF==9|DAY_DIFF==-9))
         &(SEX_IN==SEX)&((PCODE==POSTCODE)|(DL_PC_SIM==1|DL_PC_SIM==2))) %>%
  select(JW_FF_SN1_SIM,JW_FN_SN_SIM,SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>%
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>%
  filter(count_rivals==1)%>% 
  ungroup()

tenr<-rivals10 %>% 
  mutate(stage=23) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches10r<-rbind(oner,twor,threer,fourr,fiver,sixr,sevenr,eightr,niner,tenr)

rivals11<-anti_join(Indexer_pairs2,matches10r,by=("SERIAL"))
rivals11<-rivals11 %>%
  filter((SNAME1==SURNAME & FNAME1==FIRST_FORENAME
          |SNAME1==SURNAME & FNAME1==SECOND_FORENAME
          |SNAME1==PREVIOUS_SURNAME & FNAME1==SECOND_FORENAME
          |SNAME1==PREVIOUS_SURNAME & FNAME1==FIRST_FORENAME
          |SNAME1==FIRST_FORENAME & FNAME1 == SURNAME
          |SNAME1==SECOND_FORENAME & FNAME1 == SURNAME
          |SNAME1==FIRST_FORENAME & FNAME1 == PREVIOUS_SURNAME
          |SNAME1==SECOND_FORENAME & FNAME1 == PREVIOUS_SURNAME)
         &((YEAR_DIFF==0&MONTH_DIFF==0)
           |(MONTH_DIFF==0&DAY_DIFF==0)
           |(DAY_DIFF==0&YEAR_DIFF==0))
         &(SEX_IN==SEX)&((PCODE==POSTCODE))) %>%
  select(JW_FF_SN1_SIM,JW_FN_SN_SIM,SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>%
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>%
  filter(count_rivals==1)%>% 
  ungroup()
elevenr<-rivals11 %>% 
  mutate(stage=24) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches11r<-rbind(oner,twor,threer,fourr,fiver,sixr,sevenr,eightr,niner,tenr,elevenr)

rivals12<-anti_join(Indexer_pairs2,matches11r,by=("SERIAL"))
rivals12<-rivals12 %>%
  filter((SNAME1==SURNAME&JW_FF_SIM<=0.1
          |FNAME1==FIRST_FORENAME&JW_SN_SIM<=0.1
          |SNAME1==PREVIOUS_SURNAME&JW_FF_SIM<=0.1
          |FNAME1==SECOND_FORENAME&JW_SN_SIM<=0.1
          |SNAME1==SECOND_FORENAME & FNAME1==PREVIOUS_SURNAME)
         &((YEAR_DIFF==0&MONTH_DIFF==0)
           |(MONTH_DIFF==0&DAY_DIFF==0)
           |(DAY_DIFF==0&YEAR_DIFF==0))
         &(SEX_IN==SEX)&((PCODE==POSTCODE)|(DL_PC_SIM<3))) %>%
  select(JW_FF_SN1_SIM,JW_FN_SN_SIM,SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>%
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>%
  filter(count_rivals==1)%>% 
  ungroup()
twelver<-rivals12 %>% 
  mutate(stage=25) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches12r<-rbind(oner,twor,threer,fourr,fiver,sixr,sevenr,eightr,niner,tenr,elevenr,twelver)

rivals13<-anti_join(Indexer_pairs2,matches12r,by=("SERIAL"))
rivals13<-rivals13 %>%
  filter((SNAME1==SURNAME & FNAME1==FIRST_FORENAME
          |SNAME1==SURNAME & FNAME1==SECOND_FORENAME
          |SNAME1==PREVIOUS_SURNAME & FNAME1==SECOND_FORENAME
          |SNAME1==PREVIOUS_SURNAME & FNAME1==FIRST_FORENAME
          |SNAME1==FIRST_FORENAME & FNAME1 == SURNAME
          |SNAME1==SECOND_FORENAME & FNAME1 == SURNAME
          |SNAME1==FIRST_FORENAME & FNAME1 == PREVIOUS_SURNAME
          |SNAME1==SECOND_FORENAME & FNAME1 == PREVIOUS_SURNAME)
         &((YEAR_DIFF==0&MONTH_DIFF==0&between(DAY_DIFF,-1,1))
           |(MONTH_DIFF==0&DAY_DIFF==0&between(YEAR_DIFF,-5,5))
           |(DAY_DIFF==0&YEAR_DIFF==0&between(MONTH_DIFF,-1,1))
           &(SEX_IN==SEX))&((PCODE==POSTCODE)|(DL_PC_SIM==1|DL_PC_SIM<5))) %>% 
    select(JW_FF_SN1_SIM,JW_FN_SN_SIM,SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>%
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>%
  filter(count_rivals==1)%>% 
  ungroup() 

thirteenr<-rivals13 %>% 
  mutate(stage=26) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches13r<-rbind(oner,twor,threer,fourr,fiver,sixr,sevenr,eightr,niner,tenr,elevenr,twelver,thirteenr)

rivals14<-anti_join(Indexer_pairs2,matches13r,by=("SERIAL"))
rivals14a<-left_join(rivals14,joined,by=c("FNAME1"="Nicknames","FIRST_FORENAME"="Givennames"))
#CONSIDER ADDING SECOND_FORENAME MATCHES BY NICKNAME ON INCOMING
rivals14a<-rivals14a %>% 
  filter(!is.na(Givennames2)&!is.na(Nicknames2)) %>% 
  distinct()
rivals14a<-rivals14a %>% 
  mutate(JW_FN_NN1_SIM = (stringdist(Nicknames2,FNAME1, method =c("jw"),p=0.1)))%>%
  mutate(JW_FF_GN2_SIM = (stringdist(Givennames2,FIRST_FORENAME, method =c("jw"),p=0.1))) %>% 
  mutate(JW_SF_GN2_SIM = (stringdist(Givennames2,SECOND_FORENAME, method =c("jw"),p=0.1))) %>% 
  group_by(SERIAL) %>% 
  mutate(min_fn_nn1=min(JW_FN_NN1_SIM)) %>% 
  mutate(min_ff_gn2=min(JW_FF_GN2_SIM)) %>% 
  mutate(min_sf_gn2=min(JW_SF_GN2_SIM)) %>% 
  filter(min_fn_nn1==JW_FN_NN1_SIM) %>% 
  ungroup()
# distinct(SERIAL, UPI_NUMBER) %>% #get records with closesest forenames for consideration
##catalogue forename similarity assuming it is an nickname
rivals14a<-rivals14a %>% 
  filter(((JW_FN_NN1_SIM<=0.14&JW_FF_GN2_SIM<=0.14)
          |(JW_FN_NN1_SIM<=0.14&JW_SF_GN2_SIM<=0.14))
         & ((SNAME1==SURNAME|SNAME1==PREVIOUS_SURNAME
             |JW_SN_SIM<=0.1|JW_SN_PS_SIM<=0.1)
            |(JW_SN_SIM<=0.14)
            |(JW_SN_PS_SIM<=0.14))
         & ((DOB==DATE_OF_BIRTH)
            |((YEAR_DIFF==0|YEAR_DIFF==1)&(MONTH_DIFF==0|MONTH_DIFF==1))
            |((MONTH_DIFF==0|MONTH_DIFF==1)&(DAY_DIFF==0|DAY_DIFF==1))
            |((DAY_DIFF==0|DAY_DIFF==1)&(YEAR_DIFF==0|YEAR_DIFF==1)))
         &(SEX==SEX_IN)
         &((PCODE==POSTCODE)|(DL_PC_SIM==1|DL_PC_SIM==2|DL_PC_SIM==3))) %>% 
  select(SERIAL,JW_FN_NN1_SIM,JW_FF_GN2_SIM,JW_FN_NN1_SIM,JW_SF_GN2_SIM,Nicknames2,Givennames2,UPI_NUMBER,MATCH_WEIGHT,
         PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,
         SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF,
         DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
  group_by(SERIAL)%>%
  mutate(count_rivals=n()) %>%
  filter(count_rivals==1)%>% 
  ungroup()

fourteenr<-rivals14a %>% 
  mutate(stage=28) %>% 
  select(SERIAL,UPI_NUMBER,stage)
matches14r<-rbind(oner,twor,threer,fourr,fiver,sixr,sevenr,eightr,niner,tenr,elevenr,twelver,thirteenr,fourteenr)

#SDMD_SN STAGE
# rivals15<-anti_join(Indexer_pairs2,matches14r,by=("SERIAL"))
# rivals15<-rivals15 %>% 
#   filter((nchar(FNAME1)<3&nchar(SNAME1<3))|nchar(SNAME1<3)|nchar(FNAME1<3)) %>% 
#   
#   filter(((FNAME1==FF_INITIAL & SNAME1==SDMD_SN)
#           |
#             (FNAME1==SDMD_FN  & SNAME1==SDMD_SN)
#           |
#             (FNAME1==SDMD_FN_DOUBLE  & SNAME1==SDMD_SN)
#           |
#             (FNAME1==FIRST_FORENAME & SNAME1==SDMD_SN)
#           |
#             (JW_FF_SIM<=0.25 & SNAME1==SDMD_SN)#
#           |
#             (FNAME_FF_FUSED_BEGINNING<=0&SNAME1==SDMD_SN)
#           |
#             (FNAME_FF_FUSED_END<=0&SNAME1==SDMD_SN)
#           |
#             (FNAME_SF_FUSED_BEGINNING<=0&SNAME1==SDMD_SN)
#           |
#             (FNAME_SF_FUSED_END<=0&SNAME1==SDMD_SN))
#          & 
#            ((DOB==DATE_OF_BIRTH)
#             |
#               ((YEAR_DIFF==0|YEAR_DIFF==1)
#                |
#                  (MONTH_DIFF==0|MONTH_DIFF==1)
#                |
#                  (MONTH_DIFF==0|MONTH_DIFF==1)
#                |
#                  (DAY_DIFF==0|DAY_DIFF==1)
#                |
#                  (DAY_DIFF==0|DAY_DIFF==1)
#                |
#                  (YEAR_DIFF==0|YEAR_DIFF==1))
#             &
#               (SEX==SEX_IN)
#             &
#               ((PCODE==POSTCODE)|(DL_PC_SIM==1|DL_PC_SIM==2|DL_PC_SIM==3)))) %>% 
#   
#   
#   select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME, SURNAME,SNAME1,SURNAME_FUSED_BEGINNING,SURNAME_FUSED_END,SNAME_LEN_CHECK,DATE_OF_BIRTH,YEAR_DIFF,MONTH_DIFF, DAY_DIFF,DOB,SEX,SEX_IN,JW_FF_SIM,JW_SN_SIM,QGRAM_FF_SIM,QGRAM_SN_SIM,DL_FF_SIM,DL_SN_SIM,DL_PC_SIM) %>% 
#   group_by(SERIAL)%>%
#   mutate(count_rivals=n()) %>%
#   filter(count_rivals==1)%>% 
#   ungroup()
# 
# fifteenr<-rivals15 %>% 
#   mutate(stage=29) %>% 
#   select(SERIAL,UPI_NUMBER,stage)
# matches15r<-rbind(oner,twor,threer,fourr,fiver,sixr,sevenr,eightr,niner,tenr,elevenr,twelver,thirteenr,fourteenr,fifteenr)

### checkdups<-anti_join(matches15,matches13,by="SERIAL",keep=T)
### checkdups2<-anti_join(matches13,matches15,by="SERIAL",keep=T)

total_matches<-rbind(matches13,matches14r)
total_matches<-total_matches %>% 
  mutate(SERIAL=as.integer(SERIAL)) %>% 
  mutate(SERIAL=as.character(SERIAL)) %>%
  mutate(UPI_NUMBER=as.character(UPI_NUMBER)) %>%
  mutate(UPI_NUMBER=phsmethods::chi_pad(UPI_NUMBER)) 

matches<-left_join(total_matches,Indexer_pairs,by=c("UPI_NUMBER","SERIAL"))  
matches<-matches %>% 
  select(SERIAL,UPI_NUMBER,MATCH_WEIGHT,stage,PCODE,POSTCODE,FIRST_FORENAME,SECOND_FORENAME,FNAME1,PREVIOUS_SURNAME,SURNAME,SNAME1,DATE_OF_BIRTH,DOB,SEX,SEX_IN)
#plot(total_matches$stage,total_matches$MATCH_WEIGHT)

precise_matches <- tbl_df(dbGetQuery(con, statement = glue("SELECT DISTINCT
                                     ANALYSIS_DETAILS.SERIAL,
                                     ANALYSIS_DETAILS.UPI_NUMBER
                                     FROM (INDEXER.ANALYSIS_MASTER ANALYSIS_MASTER
                                     INNER JOIN INDEXER.REVIEW_CATEGORIES REVIEW_CATEGORIES
                                     ON (ANALYSIS_MASTER.CATEGORY = REVIEW_CATEGORIES.CATEGORY))
                                     INNER JOIN INDEXER.ANALYSIS_DETAILS ANALYSIS_DETAILS
                                     ON (ANALYSIS_DETAILS.STUDY_NO = ANALYSIS_MASTER.STUDY_NO)
                                     AND (ANALYSIS_DETAILS.SERIAL = ANALYSIS_MASTER.SERIAL)
                                     WHERE (REVIEW_CATEGORIES.CATEGORY_DESCRIPTION = 'Exact match')
                                     AND (ANALYSIS_DETAILS.STUDY_NO = {STUDY_NO})")))
precise_matches<-precise_matches %>% 
  mutate(stage=99)

matches<-matches %>% 
  select(SERIAL,UPI_NUMBER,stage)
my_matches<-rbind(precise_matches,matches)
my_matches<-my_matches %>% 
  mutate(SERIAL=as.integer(SERIAL)) %>% 
  mutate(SERIAL=as.character(SERIAL)) %>% 
  mutate(UPI_NUMBER=as.character(UPI_NUMBER)) %>% 
  mutate(UPI_NUMBER=phsmethods::chi_pad(UPI_NUMBER))


Indexer_pairs<-Indexer_pairs %>%
  mutate(SERIAL=as.integer(SERIAL)) %>% 
  mutate(SERIAL=as.character(SERIAL)) %>%
  mutate(UPI_NUMBER=as.character(UPI_NUMBER)) %>%
  mutate(UPI_NUMBER=phsmethods::chi_pad(UPI_NUMBER))

matches_detail<-left_join(my_matches,Indexer_pairs,c("UPI_NUMBER","SERIAL"))

# #####################################
# #Stratified review example
# library(tidyverse)
# library(Hmisc)
# strat_checks<-matches_detail %>%
# #### fake some data to demonstrate
# #fakedata <- tibble(age = rnorm(10000, 40,20), sex = sample(x = 0:1, size = 10000, replace = T)) %>%
#   filter(MATCH_WEIGHT >=14& MATCH_WEIGHT <= 25)%>%
#   mutate(SCORE_C = cut2(x = MATCH_WEIGHT, cuts = seq(14, 25, 5)))
# 
# ## sample stratified random from fakedata
# sample_per_stratum <- 100
# set.seed(1234) # ensures same sample/repeatable
# strat_checks<-strat_checks %>%
#   group_by(SCORE_C, stage) %>%
#   # this takes the smallest of the total number in the stratum (for small strata) or the chosen sample size
#   sample_n(min(length(stage), sample_per_stratum)) %>%
#   ungroup()
# ####################################


### update to correspond with batch run which has completed seeding
b<- "1"
# update to corresppond with susequent batch load - if finished, call it batch 4
ba<-"2"
### update to correspond with date on file you are seeding
bb<- "1011"

### LOAD input file for preceeding run ###
### update "XXXX" to corresponding "ddmm" for your file ###
incoming_neg<- fread(glue("/chi/(1) Project Folders/920092 - ANTIGEN Daily Tests/In Negative/NEG_LOAD{b}_{bb}.csv"))
incoming_neg<-incoming_neg %>% 
  mutate(ID=as.character(ID))
incoming_neg_2<-incoming_neg %>% 
  select(ID,TestType) %>% 
  mutate(ID=as.character(ID))

matches_detail<-left_join(incoming_neg_2,matches_detail,by=c("ID"="SERIAL"))

#NEXT RELOAD

matches_FINAL<-matches_detail %>% 
  select(ID,UPI_NUMBER,TestType) %>% 
  rename(CHINumber=UPI_NUMBER) %>% 
  mutate(CHINumber=as.character(CHINumber)) %>% 
  mutate(CHINumber=phsmethods::chi_pad(CHINumber)) %>% 
  arrange(ID)

### save out matched records for either 1st, 2nd or 3rd run
fwrite(my_matches, glue("/chi/(1) Project Folders/920092 - ANTIGEN Daily Tests/out Negative/NEG_LOAD{b}_{bb}.csv"))

#############################################################
## this section is only required for Batches that are not the final one
### don't run this line if it's the final load
unmatched_FINAL<-matches_FINAL %>% 
  filter(is.na(CHINumber)) %>% 
  select(-TestType)

### don't run this line if it's the final load
non_matches_detail<-left_join(unmatched_FINAL,incoming_neg,by="ID")%>%
  #rename(CHINumber=CHINumber.x)%>%
  select(-CHINumber)

### don't run this line if it's the final load
fwrite(non_matches_detail, glue("/chi/(1) Project Folders/920092 - ANTIGEN Daily Tests/In Negative/NEG_LOAD{ba}_{bb}.csv"))
#############################################################

#WRITE OUT TO INTEGRATION FILE PATH
# comment in/out based on number of loads
# this combines the matched results from all seeding runs to create output file ###
study_1<- fread(glue("/chi/(1) Project Folders/920092 - ANTIGEN Daily Tests/out Negative/NEG_LOAD1_{bb}.csv"))
study_2<- fread(glue("/chi/(1) Project Folders/920092 - ANTIGEN Daily Tests/out Negative/NEG_LOAD2_{bb}.csv"))
#study_3<- fread(glue("/chi/(1) Project Folders/920092 - ANTIGEN Daily Tests/out Negative/NEG_LOAD3_{bb}.csv"))

matches_across_runs<-rbind(study_1,study_2)%>% #,study_3)%>%
  rename(ID=SERIAL)%>%
  mutate(ID=as.character(ID))%>%
  mutate(UPI_NUMBER=as.character(UPI_NUMBER)) %>%
  mutate(UPI_NUMBER=phsmethods::chi_pad(UPI_NUMBER))

##Read in input file from LOAD 1 as it contains all records submitted###
incoming_neg<- fread(glue("/chi/(1) Project Folders/920092 - ANTIGEN Daily Tests/In Negative/NEG_LOAD1_{bb}.csv"))
incoming_neg<-incoming_neg %>% 
  mutate(ID=as.character(ID))

integration_file<-left_join(incoming_neg,matches_across_runs,by="ID")

## save out results to CHI personal folder, to remove break matches that have been retained by auto-pairing process##
# update with personal folder name #

bbb <- "Adebusola"
fwrite(integration_file, glue("/chi/{bbb}/NEG_{bb}.csv"))

## read in edited version of auto-pairing results with break matches retained ##

integration_file_edited<-fread(glue("/chi/{bbb}/NEG_{bb}_edited.csv"))

integration_file<-integration_file_edited %>% 
  rename(CHINumber=UPI_NUMBER)%>%
  select(ID,CHINumber,TestType)%>%#check order
  mutate(CHINumber=as.character(CHINumber)) %>%
  mutate(CHINumber=phsmethods::chi_pad(CHINumber))

#save out final result in our folder - double check in Notepad++ that these are LF (not CRLF) filetype
data.table::fwrite(integration_file,glue("/chi/(1) Project Folders/920092 - ANTIGEN Daily Tests/out Negative/","Manual_result_NEGATIVE-CHIReSeed_2021{bb}.csv"))

#save to CHI Matching > Out
data.table::fwrite(integration_file,glue("/conf/integration/CHIMatching/Out/","Manual_result_NEGATIVE-CHIReSeed_2021{bb}.csv"))
