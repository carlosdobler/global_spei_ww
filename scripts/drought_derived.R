
# source("~/00-mount.R")

source("scripts/00_setup.R")
source("scripts/write_nc.R")
source("scripts/functions_derived.R")

# plan(multisession, workers = 10)

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

walk(c("AFR", "AUS", "CAM", "CAS", "EAS", "EUR", "NAM", "SAM", "SEA", "WAS")[2], function(dom){      # ***************
  
  print(str_glue("********** PROCESSING DOM {dom} **********"))
  
  "~/bucket_mine/results/global_spei_ww/" %>%
    list.files(full.names = T) %>%
    .[str_detect(., dom)] %>%
    str_split("_", simplify = T) %>%
    .[, c(5,6)] %>%
    {str_glue("{.[,1]}_{.[,2]}")} %>%
    unique() -> mods
  
  mods[str_detect(mods, "CNRM|ICHEC", negate = T)] %>% 
  
    .[4:6] %>%                                                                                     # ************
    
    walk(function(mod){
      
      print(str_glue("PROCESSING MODEL {mod}"))
      
      walk(c("01", "02", "03", "06", "09", "12", "18", "24", "36"), function(acc){              # *****************   
        
        print(str_glue("   PROCESSING ACC {acc}"))
        
        # plan(sequential)
        plan(multicore, workers = 10)
        
        print(str_glue("      Importing into stars"))
        tic("      --Done")
        
        "~/bucket_mine/results/global_spei_ww/" %>% 
          list.files(full.names = T) %>% 
          .[str_detect(., dom)] %>% 
          .[str_detect(., mod)] %>% 
          .[str_detect(., str_glue("spei-{acc}"))] -> ff 
        
        s <- NA
        class(s) <- "try-error"
        
        while(class(s) == "try-error"){
          
          try(read_ncdf(ff, ncsub = cbind(start = c(1,1,as.integer(acc)),
                                          count = c(NA,NA,NA))) %>% 
                suppressMessages()) -> s
          
          if(class(s) == "try-error") Sys.sleep(10)
          
        }
        
        toc()
        
        # plan(multicore, workers = 10)
        
        # RAW DURATION
        
        print(str_glue("      Calculating raw duration"))
        tic("      --Done")

        s %>% 
          st_apply(c(1,2), 
                   func_dur_raw, 
                   FUTURE = T,
                   .fname = "time") -> s_dur_raw
        
        s_dur_raw %>% 
          aperm(c(2,3,1)) %>% 
          st_set_dimensions("time", values = st_get_dimension_values(s, "time")) -> s_dur_raw
        
        toc()
        
        # RAW INTENSITY

        print(str_glue("      Calculating raw intensity"))
        tic("      --Done")
        
        s %>% 
          st_apply(c(1,2), 
                   func_int_raw, 
                   FUTURE = T,
                   .fname = "time") -> s_int_raw
        
        s_int_raw %>% 
          aperm(c(2,3,1)) %>% 
          st_set_dimensions("time", values = st_get_dimension_values(s, "time")) -> s_int_raw
        
        toc()
        
        
        plan(multicore, workers = 3)
        
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
          
          
          # PROBABILITY
          
          # tic("         Probability done")
          
          fname <- str_glue("~/bucket_mine/results/global_spei_ww/derived/spei-{acc}_probability_{dom}_{str_split(mod, '_', simplify = T)[,1]}_{mod_sh}_{warm}C.nc")
          
          if(file.exists(fname)){
            
            print(str_glue(" {fname} exists!"))
            
          } else {
          
            s %>% 
              filter(year(time) >= start_end[1],
                     year(time) <= start_end[2]) %>%  
              
              st_apply(c(1,2), function(x){
                
                if(all(is.na(x))){
                  NA
                } else {
                  sum(x <= -1.6, na.rm = T)/length(x)
                }
                
              },
              FUTURE = F,
              .fname = "prob") -> s_f
            
            func_write_nc_notime(s_f,
                                 fname)
            
            }
            
          # toc()
          
          # MEAN DURATION
          
          # tic("         Duration done")
          
          fname <- str_glue("~/bucket_mine/results/global_spei_ww/derived/spei-{acc}_meanduration_{dom}_{str_split(mod, '_', simplify = T)[,1]}_{mod_sh}_{warm}C.nc")
          
          if(file.exists(fname)){
            
            print(str_glue(" {fname} exists!"))
            
          } else {
            
            s_dur_raw %>% 
              filter(year(time) >= start_end[1],
                     year(time) <= start_end[2]) %>% #.[,150,150,] %>% pull(1) %>% as.vector() -> x
              
              st_apply(c(1,2), function(x){
                
                if(all(is.na(x))){
                  NA
                } else if(all(x == -9999)){
                  0
                } else {
                  
                  x[x != -9999] %>% 
                    mean()
                  
                }
                
              },
              FUTURE = F,
              .fname = "n_months") -> s_f
            
            func_write_nc_notime(s_f,
                                 fname)
          }
          
          # toc()
          
          # MEAN INTENSITY
          
          # tic("         Intensity done")
          
          fname <- str_glue("~/bucket_mine/results/global_spei_ww/derived/spei-{acc}_meanintensity_{dom}_{str_split(mod, '_', simplify = T)[,1]}_{mod_sh}_{warm}C.nc")
          
          if(file.exists(fname)){
            
            print(str_glue(" {fname} exists!"))
            
          } else {
          
            s_int_raw %>% 
              filter(year(time) >= start_end[1],
                     year(time) <= start_end[2]) %>% #.[,150,150,] %>% pull(1) %>% as.vector() -> x
              
              st_apply(c(1,2), function(x){
                
                if(all(is.na(x))){
                  NA
                } else if(all(x == -9999)){
                  0
                } else {
                  
                  x[x != -9999] %>% 
                    mean()
                  
                }
                
              },
              FUTURE = F,
              .fname = "intensity") -> s_f
          
            func_write_nc_notime(s_f,
                                 fname)
          }
          
          
          # toc()
          # toc()
          
        })
        toc()
        
        rm(s_int_raw, s_dur_raw)
        
      })
      
    })
  
})




