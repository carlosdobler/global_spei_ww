func_dur_raw <- function(x){
  
  if(all(is.na(x))){
    rep(NA, length(x))
  } else {
    
    if(sum(is.na(x)) >= 1){
      imputeTS::na_interpolation(x, maxgap = 2) -> x
    }
    
    ifelse(x <= -1.6, 1, 0) -> a
    
    if(sum(a, na.rm = T) == 0){
      
      y <- rep(-9999, length(x))
      
    } else {
      
      rle(a) -> r
      
      if(r$values[1] != 0){
        c(0, r$values) -> r$values
        c(0, r$lengths) -> r$lengths
      }
      
      if(last(r$values) != 1){
        c(r$values, 1) -> r$values
        c(r$lengths, 0) -> r$lengths
      }
      
      seq_len(length(r$values)/2) %>% 
        rep(each = 2) -> ind
      
      aggregate(r$lengths, by = list(ind), sum)$x %>% 
        cumsum() -> pos
      
      cbind(r$values, r$lengths) %>% 
        .[.[,1] == 1, , drop = F] %>% 
        .[,2] -> len
      
      y <- rep(-9999, length(x))
      
      y[pos] <- len
      
    }
    
    return(y)
    
  }
  
}







func_int_raw <- function(x){
  
  if(all(is.na(x))){
    rep(NA, length(x))
  } else {
    
    if(sum(is.na(x)) >= 1){
      imputeTS::na_interpolation(x, maxgap = 2) -> x
    }
    
    ifelse(x <= -0.8, 1, 0) -> a
    
    if(sum(a, na.rm = T) == 0){
      
      y <- rep(-9999, length(x))
      
    } else {
      
      rle(a) -> r
      
      if(r$values[1] != 0){
        c(0, r$values) -> r$values
        c(0, r$lengths) -> r$lengths
      }
      
      if(last(r$values) != 1){
        c(r$values, 1) -> r$values
        c(r$lengths, 0) -> r$lengths
      }
      
      seq_len(length(r$values)/2) %>% 
        rep(each = 2) -> ind
      
      aggregate(r$lengths, by = list(ind), sum)$x %>% 
        cumsum() -> pos
      
      cbind(r$values, 
            r$lengths, 
            c(0, r$lengths[-length(r$lengths)] %>% cumsum %>% {.+1}),
            r$lengths %>% cumsum) %>%
        .[!is.na(.[,1]), , drop = F] %>% 
        .[.[,1] == 1, , drop = F] -> index
      
      map_dbl(seq_len(nrow(index)), function(i){
        
        x[index[i,3]:index[i,4]] %>% 
          mean()
        
      }) -> int
      
      y <- rep(-9999, length(x))
      
      y[pos] <- int
      
    }
   
    return(y) 
  }
  
}
  




# walk(seq_len(dim(s)[1]), function(lon_){
#   walk(seq_len(dim(s)[2]), function(lat_){
# 
#     print(str_glue("{lon_} - {lat_}"))
# 
#     s[,lon_,lat_,] %>% pull(1) %>% as.vector() -> x
# 
#     func_int_raw(x)
# 
#   })
# })
