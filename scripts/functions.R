
func_jump_1 <- function(x){

    count_na <- sum(is.na(x))

    if(count_na == length(x)){

      mean_seg_diff <- NA
      mean_time_bkpt <- NA
      sprd_seg_diff <- NA
      sprd_time_bkpt <- NA

    } else {

      if(count_na > 0){

        imputeTS::na_interpolation(x) -> x

      }

      # CHANGE IN MEAN

      changepoint::cpt.mean(x, class = F, minseglen = 12*10)[1] -> bkpt

      if(bkpt == length(x)){
        mean_seg_diff <- 0
        mean_time_bkpt <- NA

      } else {

        dates[bkpt] %>%
          as.integer() -> mean_time_bkpt

        scales::rescale(x, from = quantile(x, c(0.01, 0.99))) %>%
            {case_when(. > 1 ~ 1,
                       . < 0 ~ 0,
                       TRUE ~ .)} -> x_scaled

        range1 <- (bkpt-10*12):(bkpt-1) %>% .[. > 0]
        range2 <- (bkpt+1):(bkpt+10*12) %>% .[. <= 1560]

        rg1 <- x_scaled[range1] %>% na.omit() %>% mean(trim = 0.1)
        rg2 <- x_scaled[range2] %>% na.omit() %>% mean(trim = 0.1)

        mean_seg_diff <- abs(rg1 - rg2)

      }


      # CHANGE IN SPREAD

      x_cut <- x[1:(12*60)]
      changepoint::cpt.var(x_cut, class = F, minseglen = 10*12)[1] -> bkpt

      if(bkpt == length(x_cut)){
        sprd_seg_diff <- 0
        sprd_time_bkpt <- NA

      } else {

        dates[bkpt] %>%
          as.integer() -> sprd_time_bkpt

        range1 <- (bkpt-15*12-1):(bkpt-1) %>% .[. > 0]
        range2 <- (bkpt+1):(bkpt+15*12+1) %>% .[. <= 1560]

        rg1 <- x[range1] %>% na.omit() %>% {boxplot.stats(., coef = 0.95)$stats} %>% {c(first(.), last(.))} %>% diff()
        rg2 <- x[range2] %>% na.omit() %>% {boxplot.stats(., coef = 0.95)$stats} %>% {c(first(.), last(.))} %>% diff()

        if(rg1 > rg2){
          sprd_seg_diff <- 1-(rg2/rg1)

        } else {
          sprd_seg_diff <- 1-(rg1/rg2)

        }

      }

    }

    c(mean_seg_diff = mean_seg_diff,
      mean_time_bkpt = mean_time_bkpt,
      sprd_seg_diff = sprd_seg_diff,
      sprd_time_bkpt = sprd_time_bkpt,
      count_na = count_na)

}


# ***********************************************

func_jump_2 <- function(x, dates){
  
  count_na <- sum(is.na(x))
  
  if(count_na == length(x)){
    
    mean_seg_diff <- NA
    mean_time_bkpt <- NA
    sprd_seg_diff <- NA
    sprd_time_bkpt <- NA
    
  } else {
    
    if(count_na > 0){
      
      imputeTS::na_interpolation(x) -> x
      
    }
    
    # remove outliers
    # w <- 10*12+1
    # zoo::rollapply(x,
    #                FUN = function(y){
    # 
    #                  sort(y) -> y
    #                  c(y[0.25*w], y[0.5*w], y[0.75*w]) -> q
    # 
    #                  q[1]-(q[2]-q[1])*2.5 -> q1
    #                  q[3]+(q[3]-q[2])*2.5 -> q2
    #                  c(q1,q2)
    # 
    #                },
    #                width = w,
    #                by = 12,
    #                fill = NA,
    #                align = "center"
    # ) -> x_outliers
    # 
    # cbind(imputeTS::na_interpolation(x_outliers[,1]),
    #       imputeTS::na_interpolation(x_outliers[,2])) -> x_outliers
    # 
    # x_clean <- x
    # x_clean[x < x_outliers[,1] | x > x_outliers[,2]] <- NA
    # imputeTS::na_interpolation(x_clean) -> x_clean
    
    
    # CHANGE IN MEAN
    
    # changepoint::cpt.mean(x_clean, class = F, minseglen = 5*12)[1] -> bkpt
    
    x %>% 
      ts(start = 1970, frequency = 12) %>% 
      stl("periodic", t.window = 5*12) %>%
      .$time.series %>% 
      .[,"trend"] %>% 
      as.vector() -> x_trend
    
    changepoint::cpt.mean(scales::rescale(x_trend), class = F, minseglen = 5*12)[1] -> bkpt
    
    if(bkpt > 12*50){
      mean_seg_diff <- 0
      mean_time_bkpt <- NA
      
    } else {
      
      dates[bkpt] %>% 
        as.integer() -> mean_time_bkpt
      
      # scales::rescale(x_clean) -> x_scaled
      
      range1 <- (bkpt-10*12):(bkpt-1) %>% .[. > 0]
      range2 <- (bkpt+1):(bkpt+10*12)
      
      # rg1 <- x_scaled[range1] %>% mean(trim = 0.1)
      # rg2 <- x_scaled[range2] %>% mean(trim = 0.1)
      
      rg1 <- x[range1] %>%
        boxplot.stats(coef = 1.25) %>% 
        .$out %>% 
        {x[range1][!x[range1] %in% .]}
      
      rg1m <- mean(rg1)
      
      rg2 <- x[range2] %>%
        boxplot.stats(coef = 1.25) %>% 
        .$out %>% 
        {x[range2][!x[range2] %in% .]}
      
      rg2m <- mean(rg2)
      
      # mean_seg_diff <- abs(rg1 - rg2)
      
      if(rg1m > rg2m){
        mean_seg_diff <- (rg1m-rg2m)/diff(range(c(rg1,rg2)))
      } else {
        mean_seg_diff <- (rg2m-rg1m)/diff(range(c(rg1,rg2)))
      }
      
    }
    
    
    # CHANGE IN SPREAD
    
    # changepoint::cpt.var(x_clean, class = F, minseglen = 10*12)[1] -> bkpt
    changepoint::cpt.var(x, class = F, minseglen = 5*12)[1] -> bkpt
    
    if(bkpt > 12*50){
      sprd_seg_diff <- 0
      sprd_time_bkpt <- NA
      
    } else {
      
      dates[bkpt] %>% 
        as.integer() -> sprd_time_bkpt
      
      range1 <- (bkpt-10*12-1):(bkpt-1) %>% .[. > 0]
      range2 <- (bkpt+1):(bkpt+10*12+1)
      
      # rg1 <- x_clean[range1] %>% boxplot.stats(coef = 1) %>% .$stats %>% {c(first(.), last(.))} %>% diff()
      # rg2 <- x_clean[range2] %>% boxplot.stats(coef = 1) %>% .$stats %>% {c(first(.), last(.))} %>% diff()
      
      rg1 <- x[range1] %>%
        sort() %>% 
        {c(.[length(.)*1/4], .[length(.)*2/4], .[length(.)*3/4])} %>% 
        {c((.[1]-(.[2]-.[1])*1.5), (.[3]+(.[3]-.[2])*1.5))} %>% 
        {x[range1][x[range1] > .[1] & x[range1] < .[2]]} %>% 
        range() %>% 
        diff()
      
      rg2 <- x[range2] %>%
        sort() %>% 
        {c(.[length(.)*1/4], .[length(.)*2/4], .[length(.)*3/4])} %>% 
        {c((.[1]-(.[2]-.[1])*1.5), (.[3]+(.[3]-.[2])*1.5))} %>% 
        {x[range2][x[range2] > .[1] & x[range2] < .[2]]} %>% 
        range() %>% 
        diff()
        
      
      
      if(rg1 > rg2){
        sprd_seg_diff <- 1-(rg2/rg1)
        
      } else {
        sprd_seg_diff <- 1-(rg1/rg2)
        
      }
      
    }
    
  }
  
  c(mean_seg_diff = mean_seg_diff,
    mean_time_bkpt = mean_time_bkpt,
    sprd_seg_diff = sprd_seg_diff,
    sprd_time_bkpt = sprd_time_bkpt,
    count_na = count_na)
  
  
}




# ***********************************************

func_jump_3 <- function(x, dates){
  
  count_na <- sum(is.na(x))
  
  if(count_na == length(x)){
    
    trend_time_bkpt <- NA
    midp_seg_diff <- NA
    mean_seg_diff <- NA
    sprd_seg_diff <- NA
    
    sprd2_time_bkpt <- NA
    sprd2_seg_diff <- NA
    
  } else {
    
    if(count_na > 0){
      
      imputeTS::na_interpolation(x) -> x
      
    }
  
    
    # TREND ****************************
    
    
    # PT 1: BREAKPOINT DETECTION
    
    x %>% 
      ts(start = first(dates) %>% {c(year(.), month(.))}, frequency = 12) %>% 
      stl("periodic", t.window = 12*10) -> x_stl
    
    x_stl %>% 
      .$time.series %>% 
      .[, "trend"] %>% 
      as.vector() -> x_trend
    
    x_trend[1:(which(dates == "2019-12-01"))] -> x_trend_cut
    
    rpart::rpart(x_trend_cut ~ seq_along(x_trend_cut), maxdepth = 1)$where %>% 
      unname() %>% 
      diff() %>% 
      {which(. != 0)} -> bkpt
    
    if(bkpt == length(x_trend_cut)){
      trend_time_bkpt <- NA
      midp_seg_diff <- 0
      mean_seg_diff <- 0
      sprd_seg_diff <- 0
      
    } else {
      
      dates[bkpt] %>% 
        as.integer() -> trend_time_bkpt
      
      
      # PT 2: MEASURE DIFF IN MIDPOINT
      
      range1 <- (bkpt-10*12-6):(bkpt-1-6) %>% .[. > 0]
      range2 <- (bkpt+1+6):(bkpt+10*12+6)
      
      x[range1] %>%
        sort() %>% 
        {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
        {c((.[1]-(.[2]-.[1])*0.15), (.[3]+(.[3]-.[2])*0.15))} %>% 
        {x[range1][x[range1] > .[1] & x[range1] < .[2]]} -> rg1_rg
      
      rg1_rg %>%   
        range() %>% 
        {(.[2]-.[1])/2+.[1]} -> rg1
      
      x[range2] %>%
        sort() %>% 
        {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
        {c((.[1]-(.[2]-.[1])*0.15), (.[3]+(.[3]-.[2])*0.15))} %>% 
        {x[range2][x[range2] > .[1] & x[range2] < .[2]]} -> rg2_rg
      
      rg2_rg %>%
        range() %>% 
        {(.[2]-.[1])/2+.[1]} -> rg2
      
      x %>% 
        sort() %>% 
        {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
        {c((.[1]-(.[2]-.[1])*0.95), (.[3]+(.[3]-.[2])*0.95))} %>%
        {x[x > .[1] & x < .[2]]} %>% 
        range() %>% 
        diff() -> ts_diff
      
      if(anyNA(c(rg1,rg2))){
        midp_seg_diff <- 0
      } else if(rg1 > rg2){
        midp_seg_diff <- (rg1-rg2)/ts_diff
      } else {
        midp_seg_diff <- (rg2-rg1)/ts_diff
      }
      
      
      # PT 3: MEASURE DIFF IN MEAN
      rg1_rg %>%
        median() -> rg1
      
      rg2_rg %>%
        median() -> rg2
      
      if(anyNA(c(rg1,rg2))){
        mean_seg_diff <- 0
      } else if(rg1 > rg2){
        mean_seg_diff <- (rg1-rg2)/ts_diff
      } else {
        mean_seg_diff <- (rg2-rg1)/ts_diff
      }
      
      
      # PT 3: MEASURE DIFF IN SPREAD
      
      rg1_rg %>%
        range() %>% 
        diff() -> rg1
      
      rg2_rg %>%
        range() %>% 
        diff() -> rg2
      
      if(anyNA(c(rg1,rg2))){
        sprd_seg_diff <- 0
        
      } else if(rg1 > rg2){
        # sprd_seg_diff <- 1-(rg2/rg1)
        sprd_seg_diff <- rg1/ts_diff - rg2/ts_diff
        
      } else {
        # sprd_seg_diff <- 1-(rg1/rg2)
        sprd_seg_diff <- rg2/ts_diff - rg1/ts_diff
        
      }
      
    }
    
    
    # SPREAD 2 ************************
    
    changepoint::cpt.var(x, class = F, minseglen = 12*5)[1] -> bkpt
    
    if(bkpt > which(dates == "2019-12-01")){
      sprd2_seg_diff <- 0
      sprd2_time_bkpt <- NA
      
    } else {
      
      dates[bkpt] %>% 
        as.integer() -> sprd2_time_bkpt
      
      range1 <- (bkpt-15*12-6):(bkpt-1-6) %>% .[. > 0]
      range2 <- (bkpt+1+6):(bkpt+10*15+6)
      
      x[range1] %>%
        sort() %>% 
        {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
        {c((.[1]-(.[2]-.[1])*0.15), (.[3]+(.[3]-.[2])*0.15))} %>% 
        {x[range1][x[range1] > .[1] & x[range1] < .[2]]} %>% 
        range() -> rg1_rg
      
      rg1_rg %>%
        diff() -> rg1
      
      x[range2] %>%
        sort() %>% 
        {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
        {c((.[1]-(.[2]-.[1])*0.15), (.[3]+(.[3]-.[2])*0.15))} %>% 
        {x[range2][x[range2] > .[1] & x[range2] < .[2]]} %>% 
        range() -> rg2_rg
      
      rg2_rg %>%
        diff() -> rg2
      
      if(rg1 > rg2){
        # sprd2_seg_diff <- 1-(rg2/rg1)
        sprd2_seg_diff <- rg1/ts_diff - rg2/ts_diff
        
      } else {
        # sprd2_seg_diff <- 1-(rg1/rg2)
        sprd2_seg_diff <- rg2/ts_diff - rg1/ts_diff
      }
      
    }
    
  }
  
  c(trend_time_bkpt = trend_time_bkpt,
    midp_seg_diff = midp_seg_diff,
    mean_seg_diff = mean_seg_diff,
    sprd_seg_diff = sprd_seg_diff,
    
    sprd2_time_bkpt = sprd2_time_bkpt,
    sprd2_seg_diff = sprd2_seg_diff,
    count_na = count_na)
  
}



# *******************************

func_jump_3_pr <- function(x, dates){
  
  count_na <- sum(is.na(x))
  
  if(count_na == length(x)){
    
    trend_time_bkpt <- NA
    midp_seg_diff <- NA
    mean_seg_diff <- NA
    sprd_seg_diff <- NA
    
    sprd2_time_bkpt <- NA
    sprd2_seg_diff <- NA
    
  } else {
    
    if(count_na > 0){
      
      imputeTS::na_interpolation(x) -> x
      
    }
    
    
    w <- 10*12+1
    zoo::rollapply(x,
                   FUN = function(y){

                     sort(y) -> y
                     c(y[1/6*w], y[1/2*w], y[5/6*w]) -> q

                     q[1]-(q[2]-q[1])*0.5 -> q1
                     q[3]+(q[3]-q[2])*0.5 -> q2
                     c(q1,q2)

                   },
                   width = w,
                   by = 12,
                   fill = NA,
                   align = "center"
    ) -> x_outliers

    cbind(imputeTS::na_interpolation(x_outliers[,1]),
          imputeTS::na_interpolation(x_outliers[,2])) -> x_outliers

    x_clean <- x
    x_clean[x < x_outliers[,1] | x > x_outliers[,2]] <- NA
    imputeTS::na_interpolation(x_clean) -> x_clean
    
    
    x_clean %>% 
      sort() %>% 
      {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
      {c((.[1]-(.[2]-.[1])*0.95), (.[3]+(.[3]-.[2])*0.95))} %>%
      {x[x > .[1] & x < .[2]]} %>% 
      range() %>% 
      diff() -> ts_diff
    
    
    
    # TREND ****************************
    
    
    # PT 1: BREAKPOINT DETECTION
    
    x %>% 
      ts(start = first(dates) %>% {c(year(.), month(.))}, frequency = 12) %>% 
      stl("periodic", t.window = 12*10) -> x_stl
    
    x_stl %>% 
      .$time.series %>% 
      .[, "trend"] %>% 
      as.vector() -> x_trend
    
    x_trend[1:(which(dates == "2019-12-01"))] -> x_trend_cut
    
    rpart::rpart(x_trend_cut ~ seq_along(x_trend_cut), maxdepth = 1)$where %>% 
      unname() %>% 
      diff() %>% 
      {which(. != 0)} -> bkpt
    
    dates[bkpt] %>% 
      as.integer() -> trend_time_bkpt
    
    
    if(bkpt == length(x_trend_cut) | bkpt < which(dates == "1980-01-01")){
      midp_seg_diff <- 0
      mean_seg_diff <- 0
      sprd_seg_diff <- 0
      
    } else {
      
      # SEGMENTS
      range1 <- (bkpt-10*12-6):(bkpt-1-6) %>% .[. > 0]
      range2 <- (bkpt+1+6):(bkpt+10*12+6)
      
      x_clean[range1] %>%
        sort() %>% 
        {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
        {c((.[1]-(.[2]-.[1])*0.15), (.[3]+(.[3]-.[2])*0.15))} %>% 
        {x[range1][x[range1] > .[1] & x[range1] < .[2]]} -> rg1_rg
      
      x_clean[range2] %>%
        sort() %>% 
        {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
        {c((.[1]-(.[2]-.[1])*0.15), (.[3]+(.[3]-.[2])*0.15))} %>% 
        {x[range2][x[range2] > .[1] & x[range2] < .[2]]} -> rg2_rg
      
      
      # PT 2: MEASURE DIFF IN MIDPOINT
      rg1_rg %>%   
        range() %>% 
        {(.[2]-.[1])/2+.[1]} -> rg1
      
      rg2_rg %>%
        range() %>% 
        {(.[2]-.[1])/2+.[1]} -> rg2
      
      if(anyNA(c(rg1,rg2))){
        midp_seg_diff <- 0
      } else {
        midp_seg_diff <- abs(rg1-rg2)/ts_diff
      }
      
      
      # PT 3: MEASURE DIFF IN MEDIAN
      rg1_rg %>%
        median() -> rg1
      
      rg2_rg %>%
        median() -> rg2
      
      if(anyNA(c(rg1,rg2))){
        mean_seg_diff <- 0
      } else {
        mean_seg_diff <- abs(rg1-rg2)/ts_diff
      } 
      
      
      # PT 3: MEASURE DIFF IN SPREAD
      
      rg1_rg %>%
        range() %>% 
        diff() -> rg1
      
      rg2_rg %>%
        range() %>% 
        diff() -> rg2
      
      if(anyNA(c(rg1,rg2))){
        sprd_seg_diff <- 0
      } else {
        sprd_seg_diff <- abs(rg1/ts_diff - rg2/ts_diff)
      }

    }
    
    
    # SPREAD 2 ************************
    
    changepoint::cpt.var(x, class = F, minseglen = 12*5)[1] -> bkpt
    
    dates[bkpt] %>% 
      as.integer() -> sprd2_time_bkpt
    
    if(bkpt > which(dates == "2019-12-01" | bkpt < which(dates == "1980-01-01"))){
      sprd2_seg_diff <- 0
      
    } else {
      
      range1 <- (bkpt-15*12-6):(bkpt-1-6) %>% .[. > 0]
      range2 <- (bkpt+1+6):(bkpt+10*15+6)
      
      x_clean[range1] %>%
        sort() %>% 
        {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
        {c((.[1]-(.[2]-.[1])*0.15), (.[3]+(.[3]-.[2])*0.15))} %>% 
        {x[range1][x[range1] > .[1] & x[range1] < .[2]]} -> rg1_rg
      
      x_clean[range2] %>%
        sort() %>% 
        {c(.[length(.)*1/10], .[length(.)*1/2], .[length(.)*9/10])} %>% 
        {c((.[1]-(.[2]-.[1])*0.15), (.[3]+(.[3]-.[2])*0.15))} %>% 
        {x[range2][x[range2] > .[1] & x[range2] < .[2]]} -> rg2_rg
      
      rg1_rg %>%
        range() %>% 
        diff() -> rg1
      
      rg2_rg %>%
        range() %>% 
        diff() -> rg2
      
      if(anyNA(c(rg1,rg2))){
        sprd2_seg_diff <- 0
      } else {
        sprd2_seg_diff <- abs(rg1/ts_diff - rg2/ts_diff)
      }
      
    }
    
  }
  
  c(trend_time_bkpt = trend_time_bkpt,
    midp_seg_diff = midp_seg_diff,
    mean_seg_diff = mean_seg_diff,
    sprd_seg_diff = sprd_seg_diff,
    
    sprd2_time_bkpt = sprd2_time_bkpt,
    sprd2_seg_diff = sprd2_seg_diff,
    
    count_na = count_na)
  
}
