
# mass_md petfood_강아지사료

rm(list=ls())
library(dplyr)
library(DBI)
library(RMySQL)
library(writexl)
library(stringr)
library(ssh)

# SSH 터널링
cmd <- 'ssh::ssh_tunnel(ssh::ssh_connect(host = "dataqi@marketingtool.co.kr:40205", passwd = "epdlxjqi"), port = 3396, target = "127.0.0.1:3306")'
pid <- sys::r_background(
  std_out = FALSE,
  std_err = FALSE,
  args = c("-e", cmd)
)

# DB연동
con <- dbConnect(MySQL(), user="dataqi", password="dataqi123!", dbname="shield", host="localhost", port = 3396)

# Table 조회
# dbListTables(con)

# 인코딩 1차
dbSendQuery(con, "SET NAMES utf8")
dbSendQuery(con, "SET CHARACTER SET utf8")
dbSendQuery(con, "SET character_set_connection=utf8;")

# 쿼리날려 테이블 가져오기 (테이블이 커 짤라서 가져오기)
df01 = dbGetQuery(con, 'Select * from mass_md_pet_food_310_358_to where data_match=99 limit 80000')
df02 = dbGetQuery(con, 'Select * from mass_md_pet_food_310_358_to where data_match=99 limit 80000 offset 80000')
df03 = dbGetQuery(con, 'Select * from mass_md_pet_food_310_358_to where data_match=99 limit 80000 offset 160000')
df04 = dbGetQuery(con, 'Select * from mass_md_pet_food_310_358_to where data_match=99 limit 80000 offset 240000')

df <- rbind(df01, df02, df03, df04)

if (nrow(df) == nrow(df %>% distinct(seq))){
  rm(df01, df02, df03, df04)
}


# 인코딩 2차
Encoding(df[,9]) <- 'UTF-8' # top_title
Encoding(df[,10]) <- 'UTF-8' # middle_title
Encoding(df[,11]) <- 'UTF-8' # botton_title
Encoding(df[,12]) <- 'UTF-8' # below_title
Encoding(df[,18]) <- 'UTF-8' # title
Encoding(df[,19]) <- 'UTF-8' # option_name

# DB연동 종료
dbDisconnect(con)

################## 검수로직 적용 전 메타간 사전 검수 ######################
# 숫자로 형태변환을 시켜줄 칼럼 중 meta값이 두 자릿 수 이상인데 0으로 시작되는 케이스 검수
# meta_pieces, meta_bundles, meta_weight, meta_taste_1,2,3,4,5

tag_err <- ifelse(str_extract(df$meta_pieces, "^0[0-9]{0,}"), df$tag_err <- "pieces_err",
                  ifelse(str_extract(df$meta_bundles, "^0[0-9]{0,}"), df$tag_err <- "bundles_err",
                         ifelse(str_extract(df$meta_weight, "^0[0-9]{0,}"), df$tag_err <-"weight_err", 
                                ifelse(str_extract(df$meta_size, "^0[0-9]{0,}"), df$tag_err <-"size_err",
                                       ifelse(str_extract(df$meta_breed, "^0[0-9]{0,}"), df$tag_err <-"breed_err",
                                              ifelse(str_extract(df$meta_taste_1, "^0[0-9]{0,}"), df$tag_err <-"taste1_err",
                                                     ifelse(str_extract(df$meta_taste_2, "^0[0-9]{0,}"), df$tag_err <-"taste2_err",
                                                            ifelse(str_extract(df$meta_taste_3, "^0[0-9]{0,}"), df$tag_err <-"taste3_err",
                                                                   ifelse(str_extract(df$meta_taste_4, "^0[0-9]{0,}"), df$tag_err <-"taste4_err",
                                                                          ifelse(str_extract(df$meta_taste_5, "^0[0-9]{0,}"), df$tag_err <-"taste5_err",df$tag_err <-"NA"))))))))))

df$tag_err <- tag_err
check_pbw <- df %>% 
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, sm_active, multi_meta, multi_meta_json, multi_meta_count, meta_box, meta_size, meta_type, meta_breed, meta_combo, meta_grade, meta_pieces, meta_sample, 
         meta_weight, meta_bundles, meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, meta_weight_type) %>% 
  filter(tag_err!="NA")

################# 필요한 칼럼만 사용####################
df1 <- df %>% 
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, sm_active, multi_meta, multi_meta_json, multi_meta_count, meta_box, meta_size, meta_type, meta_breed, meta_combo, meta_grade, meta_pieces, meta_sample, 
         meta_weight, meta_bundles, meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, meta_weight_type) %>% 
  filter(sm_active=="1")


################## 1. set 값 검수 ######################
# hashcode count
df_key <- df1 %>% 
  count(hashcode)

# hashcode count와 df 합병
df_merge <- merge(df1, df_key, by="hashcode", all.x = TRUE)

# 합병된 데이터 중 set가 2값 이상인 데이터 추출
df_set <- df_merge %>% 
  filter(n>=2)

# 필요한 칼럼만 뽑아 set상품 체크
set_check <- df_set %>% 
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, multi_meta_json, multi_meta_count, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_type, meta_grade, meta_box, meta_sample, meta_size, meta_breed, 
         meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, n)

# 정상 시퀀스 제외
# 예외시퀀스 정리파일 불러오기
seq_except <- read.csv("D:/김정민/타고객사/로얄캐닌/예외처리/set_seq_Exception_210826.csv")

# unqiue 를 사용해서 벡터의 중복된 값을 제거한다
se <- unique(seq_except$dogfood)

# 합병하기 위해 mat 구조로 변환
rows_seq <- length(se)
mat_seq <- matrix(nrow=rows_seq, ncol=2)
mat_seq[,1] <- se
mat_seq[,2] <- "O"
colnames(mat_seq) <- c("seq","OX") # 이름 삽입

# df1_1_result 와 he를 합병 후 he값이 null인 데이터만 추출 = 예외가 안된 데이터
merge_set <- merge(set_check, mat_seq, by="seq", all.x = TRUE)

check_set <- merge_set %>% 
  filter(is.na(OX))

################## 2. multi meta 검수 ######################

# 2-1. 멀티메타 값 풀기
# 작업이 많아 multi_meta_json에 값이 있는 것들만 추출
# df %>% count(multi_meta_count)
df2 <- df1 %>% 
  filter(multi_meta_count>=2) %>% 
  filter(multi_meta==1)

# json 칼럼만 추출
mtjsn <- df2$multi_meta_json

# 필요없는 문자열 치환(제거)
mtjsn_1 <- str_replace_all(mtjsn,pattern = '"', replacement = '')
mtjsn_10 <- gsub("weight_type", "wtt", mtjsn_1)
mtjsn_11 <- gsub("taste_1", "taste_a", mtjsn_10)
mtjsn_12 <- gsub("taste_2", "taste_b", mtjsn_11)
mtjsn_13 <- gsub("taste_3", "taste_c", mtjsn_12)
mtjsn_14 <- gsub("taste_4", "taste_d", mtjsn_13)
mtjsn_15 <- gsub("taste_5", "taste_e", mtjsn_14)
mtjsn_16 <- gsub("meta_set", "", mtjsn_15)
mtjsn_17 <- gsub("multi_meta_json", "", mtjsn_16)
mtjsn_18 <- gsub("multi_meta_count", "", mtjsn_17)
mtjsn_19 <- gsub("[[]","",mtjsn_18)
mtjsn_19 <- gsub("[]]","",mtjsn_19)
mtjsn_19 <- gsub("[{]","",mtjsn_19)
mtjsn_19 <- gsub("[}]","",mtjsn_19)


# ','로 구분하여 각 array 구분짓기
mtjsn_2 <- str_split(mtjsn_19, ",")

mtjsn_2[[1092]]
# mtjsn_20[1]

# 각 값 넣을 dataframe 생성
df_pc <- data.frame(matrix(nrow=0,ncol=0))
df_bd <- data.frame(matrix(nrow=0,ncol=0))
df_w <- data.frame(matrix(nrow=0,ncol=0))
df_wt <- data.frame(matrix(nrow=0,ncol=0))
df_t <- data.frame(matrix(nrow=0,ncol=0))
df_gr <- data.frame(matrix(nrow=0,ncol=0))
df_bx <- data.frame(matrix(nrow=0,ncol=0))
df_sp <- data.frame(matrix(nrow=0,ncol=0))
df_s <- data.frame(matrix(nrow=0,ncol=0))
df_b <- data.frame(matrix(nrow=0,ncol=0))
df_t1 <- data.frame(matrix(nrow=0,ncol=0))
df_t2 <- data.frame(matrix(nrow=0,ncol=0))
df_t3 <- data.frame(matrix(nrow=0,ncol=0))
df_t4 <- data.frame(matrix(nrow=0,ncol=0))
df_t5 <- data.frame(matrix(nrow=0,ncol=0))

# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기 meta_pieces
for(i in 1:nrow(df2)){
  jj <- grep("meta_pieces", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_pc[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_bundles", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_bd[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_weight", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_w[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_wtt", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_wt[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_type", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_t[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_grade", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_gr[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_box", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_bx[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_sample", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_sp[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_size", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_s[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_breed", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_b[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_taste_a", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_t1[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_taste_b", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_t2[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_taste_c", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_t3[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_taste_d", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_t4[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 문자열에서 메타값만 추출하여 반복문으로 matrix에 값 넣기
for(i in 1:nrow(df2)){
  jj <- grep("meta_taste_e", mtjsn_2[[i]])
  k <- 1
  for(j in jj){
    df_t5[i,k] <- str_extract(mtjsn_2[[i]][j], "\\d+\\.*\\d*")
    k <- k+1
  }
}


# 만든 데이터 합병
df_3 <- cbind(df_pc, df_bd, df_w, df_wt, df_t, df_gr, df_bx, df_sp, df_s, df_b, df_t1, df_t2, df_t3, df_t4, df_t5)

# 합병 후 칼럼별 정리 (1,6,11,16 ... 2,7,12,17 이런식으로 칼럼 넣어야되서 반복문 사용)
colmax <- ncol(df_t)
x <- 0
for (i in 1:colmax){
  j <- seq(from=i, to=15*colmax, by=colmax)
  x <- c(x,j)
}

df_4 <- df_3[x]

########################################################### 1차 확인 ##############################################################
# 칼럼 수 확인
colmax
# 4

#################################################### 칼럼 수에 맞춰 파생변수 생성 #################################################
###################################################################################################################################
###################################################################################################################################


# 변수 생성 (colmax = 4 에 맞추어 파생변수 4개 생성)
# 변수 생성
df_5 <- df_4 %>% 
  mutate(cols1=paste(V1, V1.1, V1.2, V1.3, V1.4, V1.5, V1.6, V1.7, V1.8, V1.9, V1.10, V1.11, V1.12, V1.13, V1.14, sep = ",")) %>% 
  mutate(cols2=paste(V2, V2.1, V2.2, V2.3, V2.4, V2.5, V2.6, V2.7, V2.8, V2.9, V2.10, V2.11, V2.12, V2.13, V2.14, sep = ",")) %>% 
  mutate(cols3=paste(V3, V3.1, V3.2, V3.3, V3.4, V3.5, V3.6, V3.7, V3.8, V3.9, V3.10, V3.11, V3.12, V3.13, V3.14, sep = ",")) %>% 
  mutate(cols4=paste(V4, V4.1, V4.2, V4.3, V4.4, V4.5, V4.6, V4.7, V4.8, V4.9, V4.10, V4.11, V4.12, V4.13, V4.14, sep = ",")) %>% 
  select(cols1, cols2, cols3, cols4)

# df2 데이터와 결합
df_6 <- cbind(df2[,c(1:8,12)], df_5)


# 값 넣을 dataframe 생성
df7 <- data.frame(matrix(nrow=0, ncol=0))

# 값별로 행 늘려주기
for (loop_col in 10:13){
  input_col <- df_6[,c(1:9,loop_col)]
  names(input_col)[10] <- "cols"
  df7 <- rbind(df7, input_col)
}

# cols 칼럼이 NA인 값 제거
df8 <- df7 %>% 
  filter(cols!="NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA")

# 푼 다중메타 수와 멀티메타카운트의 합 일치여부 확인
sum(df2$multi_meta_count) == nrow(df8)

# 콤마로 묶인 값 펼치기
spl <- str_split(df8$cols, ",")
# spl[[1]][1]

# 펼친 값 넣을 데이터 프레임 생성
df9 <- data.frame(matrix(nrow=nrow(df8),ncol=15))

# 펼친 값 다시 넣어주기
for (i in 1:nrow(df8)){
  for (j in 1:15){
    df9[i,j] <- spl[[i]][j]
  }
}

# data 합쳐주기
df10 <- cbind(df8[,c(1:9)], df9)

# 칼럼에 이름 넣어주기
colnames(df10)[10] <- "meta_pieces"
colnames(df10)[11] <- "meta_bundles"
colnames(df10)[12] <- "meta_weight"
colnames(df10)[13] <- "meta_weight_type"
colnames(df10)[14] <- "meta_type"
colnames(df10)[15] <- "meta_grade"
colnames(df10)[16] <- "meta_box"
colnames(df10)[17] <- "meta_sample"
colnames(df10)[18] <- "meta_size"
colnames(df10)[19] <- "meta_breed"
colnames(df10)[20] <- "meta_taste_1"
colnames(df10)[21] <- "meta_taste_2"
colnames(df10)[22] <- "meta_taste_3"
colnames(df10)[23] <- "meta_taste_4"
colnames(df10)[24] <- "meta_taste_5"

# 칼럼값에 null 있는지 여부 확인
multi_na <-  ifelse(df10$meta_box=="NA" | df10$meta_size=="NA" | df10$meta_type=="NA" | df10$meta_breed=="NA" | df10$meta_grade=="NA" | df10$meta_pieces=="NA" | df10$meta_sample=="NA" | df10$meta_weight=="NA" | df10$meta_bundles=="NA" | 
                      df10$meta_taste_1=="NA" | df10$meta_taste_2=="NA" | df10$meta_taste_3=="NA" | df10$meta_taste_4=="NA" | df10$meta_taste_5=="NA" | df10$meta_weight_type=="NA",  multi_na <- "O", multi_na <- "")
df10$multi_na <- multi_na

check_multi_na <- df10 %>% 
  filter(multi_na=="NA")

# 2-2. 멀티메타 이상여부 검수
# 1. multi_meta_count가 1이면서 multi_meta가 1인 값 체크
check_multi_1 <- df1 %>% 
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, sm_active, multi_meta, multi_meta_json, multi_meta_count, meta_box, meta_size, meta_type, meta_breed, meta_combo, meta_grade, meta_pieces, meta_sample, 
         meta_weight, meta_bundles, meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, meta_weight_type) %>% 
  filter(multi_meta_count=="1" & multi_meta=="1")

# 2. multi_meta 모든 값이 같은 데이터 확인
# 모든 메타 조합으로 key값 생성
dataf <- df10 %>% 
  mutate(key=paste(hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, multi_meta_count, meta_box, meta_size, meta_type, meta_breed, meta_grade, meta_pieces, meta_sample, 
                   meta_weight, meta_bundles, meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5, meta_weight_type, sep="|"))

# key 카운트
dataf_cnt <- dataf %>% 
  count(key)

# key값 기준 합병
merge_multi <- merge(dataf, dataf_cnt, by="key", all.x = TRUE)

# key가 2개 이상인 값 추출
check_multi_2 <- merge_multi %>% 
  filter(n>1)


################## 3. meta 값 검수 ######################
# 칼럼명 순서대로 정렬 + 일부 메타값만 숫자형으로 치환
df1_1 <- df1 %>% 
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, multi_meta_count, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_type, meta_grade, meta_box, meta_sample, 
         meta_size, meta_breed, meta_taste_1, meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5)

# 다중메타 데이터 제거 후 2에서 생성한 df8 rbind 하기
df1_2 <- df1_1 %>% 
  filter(multi_meta_count<=1)

df10_1 <- df10[,1:24]

# 합친 후 칼럼 정렬 (meta_combo 필요없으므로 제거)
df1_3 <- rbind(df1_2, df10_1) %>% 
  select(seq, hashcode, top_title, middle_title, bottom_title, below_title, title, option_name, multi_meta_count, meta_pieces, meta_bundles, meta_weight, meta_weight_type, meta_type, meta_grade, meta_box, meta_sample, meta_size, meta_breed, meta_taste_1
         , meta_taste_2, meta_taste_3, meta_taste_4, meta_taste_5)


# meta값 숫자형으로 변환
df1_3$meta_pieces <- as.numeric(df1_3$meta_pieces)
df1_3$meta_bundles <- as.numeric(df1_3$meta_bundles)
df1_3$meta_weight <- as.numeric(df1_3$meta_weight)
df1_3$meta_box <- as.numeric(df1_3$meta_box)
df1_3$meta_size <- as.numeric(df1_3$meta_size)
df1_3$meta_breed <- as.numeric(df1_3$meta_breed)
df1_3$meta_taste_1 <- as.numeric(df1_3$meta_taste_1)
df1_3$meta_taste_2 <- as.numeric(df1_3$meta_taste_2)
df1_3$meta_taste_3 <- as.numeric(df1_3$meta_taste_3)
df1_3$meta_taste_4 <- as.numeric(df1_3$meta_taste_4)
df1_3$meta_taste_5 <- as.numeric(df1_3$meta_taste_5)

# meta 값 검수
# pieces가 9999보다 크거나 0인 값
tag_pieces <-  ifelse(df1_3$meta_pieces > 9999 | df1_3$meta_pieces==0, tag_pieces <- "O", tag_pieces <- "")
df1_3$tag_pieces <- tag_pieces

# bundles가 9999보다 크거나 0인 값
tag_bundles <-  ifelse(df1_3$meta_bundles > 9999 | df1_3$meta_bundles==0 , tag_bundles <- "O", tag_bundles <- "")
df1_3$tag_bundles <- tag_bundles

# meta_weight
# meta_weight_type이 1이면서 meta_weight가 1000이상인 값
# meta_weight_type이 2이면서 meta_weight가 100이상인 값
# meta_weight_type이 3이면서 meta_weight가 1000이상인 값
# meta_weight_type이 4이면서 meta_weight가 100이상인 값
# meta_weight_type 이 9999 이면서 meta_weight가 9999가 아닌 값
# meta_weight_type 이 9999가 아니면서 meta_weight가 9999 인 값
tag_weight <-  ifelse(df1_3$meta_weight_type=="1" & df1_3$meta_weight >=1000 | df1_3$meta_weight_type=="2" & df1_3$meta_weight >=100 | df1_3$meta_weight_type=="3" & df1_3$meta_weight >=1000 |df1_3$meta_weight_type=="4" & df1_3$meta_weight >=100 |
                        df1_3$meta_weight_type=="9999" & df1_3$meta_weight!=9999 | df1_3$meta_weight_type!="9999" & df1_3$meta_weight==9999, tag_weight <- "O", tag_weight <- "")
df1_3$tag_weight <- tag_weight

# meta_weight_type이 1,2,3,4,9999 외 값
tag_weight_type <- ifelse(df1_3$meta_weight_type!="1" & df1_3$meta_weight_type!="2" & df1_3$meta_weight_type!="3" & df1_3$meta_weight_type!="4" & df1_3$meta_weight_type!="9999", tag_weight_type <- "O", tag_weight_type <- "")
df1_3$tag_weight_type <- tag_weight_type

# meta_type 이 1,2,3,4,5,6,7,8,9,10,11,12 외 값
tag_type <- ifelse(df1_3$meta_type!="1" & df1_3$meta_type!="2" & df1_3$meta_type!="3" & df1_3$meta_type!="4" & df1_3$meta_type!="5" & df1_3$meta_type!="6" & df1_3$meta_type!="7" & df1_3$meta_type!="8" & df1_3$meta_type!="9" & df1_3$meta_type!="10"
                   & df1_3$meta_type!="11" & df1_3$meta_type!="12" & df1_3$meta_type!="13" & df1_3$meta_type!="14", tag_type <- "O", tag_type <- "")
df1_3$tag_type <- tag_type

# meta_grade 가 1,2,3,4,5,6,7,8,9,10,11,12,139999 외 값
tag_grade <- ifelse(df1_3$meta_grade!="1" & df1_3$meta_grade!="2" & df1_3$meta_grade!="3" & df1_3$meta_grade!="4" & df1_3$meta_grade!="5" & df1_3$meta_grade!="6" & df1_3$meta_grade!="7" & df1_3$meta_grade!="8" & df1_3$meta_grade!="9" & df1_3$meta_grade!="10" & 
                      df1_3$meta_grade!="11" & df1_3$meta_grade!="12" & df1_3$meta_grade!="13" & df1_3$meta_grade!="9999", tag_grade <- "O", tag_grade <- "")
df1_3$tag_grade <- tag_grade

# meta_box가 0이거나 9999 초과 값
tag_box <-  ifelse(df1_3$meta_box==0 | df1_3$meta_box > 9999, tag_box <- "O", tag_box <- "")
df1_3$tag_box <- tag_box

# meta_sample이 1,9999 외 값
tag_sample <-  ifelse(df1_3$meta_sample!="1" & df1_3$meta_sample!="9999", tag_sample <- "O", tag_sample <- "")
df1_3$tag_sample <- tag_sample

# meta_size가 1미만 or 24 초과이며 9999가 아닌 값
tag_size <- ifelse(df1_3$meta_size >24 & df1_3$meta_size!=9999 | df1_3$meta_size < 1, tag_size <- "O", tag_size <-"")
df1_3$tag_size <- tag_size

# meta_breed가 1미만 or 15 초과이며 9999가 아닌 값
tag_breed <- ifelse(df1_3$meta_breed > 15 & df1_3$meta_breed!=9999 | df1_3$meta_breed < 1, tag_breed <- "O", tag_breed <-"")
df1_3$tag_breed <- tag_breed

# meta_taste_1가 89이상이며 9999가 아닌 값 or 1 미만인 값
tag_taste_1 <- ifelse(df1_3$meta_taste_1 >=89 & df1_3$meta_taste_1!=9999 | df1_3$meta_taste_1 < 1, tag_taste_1 <- "O", tag_taste_1 <- "")
df1_3$tag_taste_1 <- tag_taste_1

# meta_taste_2가 89이상이며 9999가 아닌 값 or 1 미만인 값
tag_taste_2 <- ifelse(df1_3$meta_taste_2 >=89 & df1_3$meta_taste_2!=9999 | df1_3$meta_taste_2 < 1, tag_taste_2 <- "O", tag_taste_2 <- "")
df1_3$tag_taste_2 <- tag_taste_2

# meta_taste_3가 89이상이며 9999가 아닌 값 or 1 미만인 값
tag_taste_3 <- ifelse(df1_3$meta_taste_3 >=89 & df1_3$meta_taste_3!=9999 | df1_3$meta_taste_3 < 1, tag_taste_3 <- "O", tag_taste_3 <- "")
df1_3$tag_taste_3 <- tag_taste_3

# meta_taste_4가 89이상이며 9999가 아닌 값 or 1 미만인 값
tag_taste_4 <- ifelse(df1_3$meta_taste_4 >=89 & df1_3$meta_taste_4!=9999 | df1_3$meta_taste_4 < 1, tag_taste_4 <- "O", tag_taste_4 <- "")
df1_3$tag_taste_4 <- tag_taste_4

# meta_taste_5가 89이상이며 9999가 아닌 값 or 1 미만인 값
tag_taste_5 <- ifelse(df1_3$meta_taste_5 >=89 & df1_3$meta_taste_5!=9999 | df1_3$meta_taste_5 < 1, tag_taste_5 <- "O", tag_taste_5 <- "")
df1_3$tag_taste_5 <- tag_taste_5

# meta값들 중 na값 존재여부
tag_na <- ifelse(is.na(df1_3$meta_pieces) | is.na(df1_3$meta_bundles) | is.na(df1_3$meta_weight) | df1_3$meta_weight_type=="NA" | df1_3$meta_type=="NA" | df1_3$meta_grade=="NA" | is.na(df1_3$meta_box) | df1_3$meta_sample=="NA" | is.na(df1_3$meta_size) |
                   is.na(df1_3$meta_breed) | is.na(df1_3$meta_taste_1) | is.na(df1_3$meta_taste_2) | is.na(df1_3$meta_taste_3) | is.na(df1_3$meta_taste_4) | is.na(df1_3$meta_taste_5), tag_na <- "O", tag_na <- "")
df1_3$tag_na <- tag_na

# 오데이터만 추출
df1_3_result <- df1_3 %>% 
  filter(tag_pieces=="O" | tag_bundles=="O" | tag_weight=="O" | tag_weight_type=="O" | tag_type=="O" | tag_grade=="O" | tag_box=="O" | tag_sample=="O" | tag_size=="O" | tag_breed=="O" | 
           tag_taste_1=="O" | tag_taste_2=="O" | tag_taste_3=="O" | tag_taste_4=="O" | tag_taste_5=="O" | tag_na=="O")

# 정상해시 제외
# 예외해시 정리파일 불러오기
hash_except <- read.csv("D:/김정민/타고객사/로얄캐닌/예외처리/Meta_Hashcode_Exception_210826.csv")

# unqiue 를 사용해서 벡터의 중복된 값을 제거한다
he <- unique(hash_except$dogfood)

# 합병하기 위해 mat 구조로 변환
rows_hash <- length(he)
mat_hash <- matrix(nrow=rows_hash, ncol=2)
mat_hash[,1] <- he
mat_hash[,2] <- "O"
colnames(mat_hash) <- c("hashcode","OX") # 이름 삽입

# df1_1_result 와 he를 합병 후 he값이 null인 데이터만 추출 = 예외가 안된 데이터
merge_meta <- merge(df1_3_result, mat_hash, by="hashcode", all.x = TRUE)

check_meta <- merge_meta %>% 
  filter(is.na(OX))

d <- Sys.Date()


# 추출
if (nrow(check_set) > 0) {
  write_xlsx(check_set, paste0("D:/김정민/타고객사/로얄캐닌/mass_md_강아지사료/2022/PetFood_dogfood_DPcheck_result_",d,".xlsx"))  
}

if (nrow(check_multi_1) > 0) {
  write_xlsx(check_multi_1, paste0("D:/김정민/타고객사/로얄캐닌/mass_md_강아지사료/2022/PetFood_dogfood_Multicheck_1_result_",d,".xlsx"))
}

if (nrow(check_multi_2) > 0) {
  write_xlsx(check_multi_2, paste0("D:/김정민/타고객사/로얄캐닌/mass_md_강아지사료/2022/PetFood_dogfood_Multicheck_2_result_",d,".xlsx"))
}

if (nrow(check_meta) > 0) {
  write_xlsx(check_meta, paste0("D:/김정민/타고객사/로얄캐닌/mass_md_강아지사료/2022/PetFood_dogfood_metacheck_result_",d,".xlsx"))
}

# 다중메타 푼것만 추출
# write_xlsx(df8, "D:/김정민/타고객사/로얄캐닌/mass_md_강아지사료/PetFood_dogfood_multimeta_0707_1.xlsx")
