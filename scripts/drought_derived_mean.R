
# source("~/00-mount.R")

source("scripts/setup.R")
source("scripts/write_nc.R")
source("scripts/functions_derived.R")

options(future.fork.enable = T)
# plan(multicore, workers = 6)
plan(sequential)

"~/bucket_mine/results/global_spei_ww/derived" -> dir_derived
# dir.create(dir_derived)

read_delim("~/bucket_mine/misc_data/CMIP5_model_temp_thresholds.csv") -> thresholds

thresholds %>% 
  select(1:6) %>% 
  pivot_longer(-Model, names_to = "Warm") %>% 
  mutate(Warm = str_sub(Warm, 3)) -> thresholds

thresholds %>% 
  mutate(Warm = ifelse(str_length(Warm) == 1, str_glue("{Warm}.0"), Warm)) -> thresholds



# ********************

walk(c("AFR", "AUS", "CAM", "CAS", "EAS", "EUR", "NAM", "SAM", "SEA", "WAS"), function(dom){      # ***************
  
  print(str_glue("********** PROCESSING DOM {dom} **********"))
  
  "~/bucket_mine/results/global_spei_ww/" %>%
    list.files(full.names = T) %>%
    .[str_detect(., dom)] %>%
    str_split("_", simplify = T) %>%
    .[, c(5,6)] %>%
    {str_glue("{.[,1]}_{.[,2]}")} %>%
    unique() -> mods
  
  mods[str_detect(mods, "CNRM|ICHEC", negate = T)] %>%
    
    walk(function(mod){
      
      print(str_glue("PROCESSING MODEL {mod}"))
      
      walk(c("03", "06", "12", "18")[3], function(acc){              # *****************   
        
        print(str_glue("   PROCESSING ACC {acc}"))
        
        # plan(sequential)
        # plan(multicore, workers = 10)
        
        print(str_glue("      Importing into stars"))
        tic("      --Done")
        
        "~/bucket_mine/results/global_spei_ww" %>% 
          list.files(full.names = T) %>% 
          .[str_detect(., dom)] %>% 
          .[str_detect(., mod)] %>% 
          .[str_detect(., str_glue("spei-{acc}"))] %>% 
          str_sub(27) %>% 
          {str_glue("gs://clim_data_reg_useast1/{.}")} -> ff
        
        # Sys.sleep(3)
        print(ff)
          
        system(str_glue("gsutil cp {ff} tmp.nc"))
        
        # 
        # s <- NA
        # class(s) <- "try-error"
        # 
        # while(class(s) == "try-error"){
        #   
        #   try(read_ncdf(ff, ncsub = cbind(start = c(1,1,as.integer(acc)),
        #                                   count = c(NA,NA,NA))) %>% 
        #         suppressMessages()) -> s
        #   
        #   if(class(s) == "try-error") Sys.sleep(10)
        #   
        # }
        
        read_ncdf("tmp.nc", ncsub = cbind(start = c(1,1,as.integer(acc)),
                                          count = c(NA,NA,NA))) %>%
          suppressMessages() -> s
        
        toc()
        
        
        
        # plan(multicore, workers = 3)
        
        print(str_glue("      Processing 6 warming levels"))
        tic(str_glue("      └--Done"))
        
        future_walk(c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0"), function(warm){
          
          
          # print(str_glue("      Processing warming level {warm}"))
          # tic("      --Done with wl")
          
          mod %>%
            str_split("_", simplify = T) %>% 
            .[,ncol(.)] %>% 
            str_split("-", simplify = T) -> mod_sh
          
          
          if(str_detect(mod, "MPI")){
            mod_sh %>% 
              {str_glue("{.[,3]}-{.[,4]}-{.[,5]}")} -> mod_sh
          } else if(str_detect(mod, "_MIROC-")){
            mod_sh %>% 
              .[,2] -> mod_sh
          } else {
            mod_sh %>% 
              {str_glue("{.[,(ncol(.)-1)]}-{.[,ncol(.)]}")} -> mod_sh
          }
          
          
          if(warm == 0.5){
            c(1971, 2000) -> start_end
            
          } else {
            
            thresholds %>% 
              filter(Model == mod_sh) %>% 
              filter(Warm == warm) %>% 
              pull(value) %>% 
              {c(.-10, .+10)} -> start_end
            
          }
          
          
          s %>% 
            filter(year(time) >= start_end[1],
                   year(time) <= start_end[2]) -> ss
          
          
          
          # MEAN
          
          fname <- str_glue("~/bucket_mine/results/global_spei_ww/derived/spei-{acc}_mean_{dom}_{str_split(mod, '_', simplify = T)[,1]}_{mod_sh}_{warm}C.nc")
          
          if(file.exists(fname)){
            
            print(str_glue(" {fname} exists!"))
            
          } else {
            
            ss %>%
              st_apply(c(1,2), function(x){
                
                if(all(is.na(x))){
                  NA
                } else {
                  mean(x, na.rm = T)
                }
                
              },
              FUTURE = F,
              .fname = "spei") -> s_f
            
            func_write_nc_notime(s_f,
                                 fname)
            
          }
          
          # toc()
          
          # 25 perc
          
          # tic("         Duration done")
          
          fname <- str_glue("~/bucket_mine/results/global_spei_ww/derived/spei-{acc}_25perc_{dom}_{str_split(mod, '_', simplify = T)[,1]}_{mod_sh}_{warm}C.nc")
          
          if(file.exists(fname)){
            
            print(str_glue(" {fname} exists!"))
            
          } else {
            
            ss %>%
              st_apply(c(1,2), function(x){
                
                if(all(is.na(x))){
                  NA
                } else {
                  quantile(x, prob = 0.25, na.rm = T)
                }
                
              },
              FUTURE = F,
              .fname = "spei") -> s_f
            
            func_write_nc_notime(s_f,
                                 fname)
          }
          
          # toc()
          
          # 75th PERC
          
          # tic("         Intensity done")
          
          fname <- str_glue("~/bucket_mine/results/global_spei_ww/derived/spei-{acc}_75perc_{dom}_{str_split(mod, '_', simplify = T)[,1]}_{mod_sh}_{warm}C.nc")
          
          if(file.exists(fname)){
            
            print(str_glue(" {fname} exists!"))
            
          } else {
            
            ss %>%
              st_apply(c(1,2), function(x){
                
                if(all(is.na(x))){
                  NA
                } else {
                  quantile(x, prob = 0.75, na.rm = T)
                }
                
              },
              FUTURE = F,
              .fname = "spei") -> s_f
            
            func_write_nc_notime(s_f,
                                 fname)
          }
          
          
          # toc()
          # toc()
          
        })
        toc()
        
        file.remove("tmp.nc")
        
        
      })
      
    })
  
})




