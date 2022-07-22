"~/bucket_mine/remo/daily/NAM-22" -> d
# dir.create(d)

# vars %>% 
#   walk(~dir.create(str_glue("{d}/{.x}")))

# move
vars %>% 
  # .[-c(1,6)] %>% 
  walk(function(v){
    
    list.files("~/bucket_mine/remo/daily", full.names = T) %>% 
      .[str_detect(., v)] %>% 
      
      future_walk(function(f, d_ = d, v_ = v){
        
        f %>% 
          str_split("/", simplify = T) %>% 
          .[,ncol(.)] -> ff
        
        file.rename(from = f, to = str_glue("{d_}/{v_}/{ff}"))
        
      })
      
  })



vars %>% 
  walk(function(v){
    
    print(v)
    
    str_glue("{d}/{v}") %>% 
      list.files(full.names = T) %>% 
      
      # future_walk(function(f, v_ = v){
      walk(function(f, v_ = v){  
      
        f %>% 
          str_split("/", simplify = T) %>% 
          .[,ncol(.)] %>% 
          str_split("_", simplify = T) -> ff
      
        ff[,8] <- "mon"
        
        ff[,9] %>% 
          str_sub(end = -4) %>% 
          {c(str_sub(., end = 6), str_sub(., start = 10, end = 15))} %>% 
          str_flatten("-") -> ff[,9]
          
        str_flatten(ff, "_") -> ff
        
        str_glue("~/bucket_mine/remo/monthly/NAM-22/{v_}/{ff}") -> ff
        
        system(str_glue("cdo monmean {f} {ff}"),
               ignore.stdout = TRUE, ignore.stderr = TRUE)
        
        print(f)
        
      })
    
  })




list.dirs("~/bucket_mine/remo/monthly/NAM-22", recursive = F) %>% 
  walk(function(d){
    list.files(d, full.names = T) %>% 
      .[str_detect(., "RegCM4")] %>% 
      walk(file.remove)
  })
