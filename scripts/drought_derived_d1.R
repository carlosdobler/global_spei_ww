
# source("~/00-mount.R")

source("scripts/00_setup.R")
source("scripts/write_nc.R")
source("scripts/functions_derived.R")

# plan(multisession, workers = 10)

"~/bucket_mine/results/global_spei_ww/derived" -> dir_derived
# dir.create(dir_derived)

drought_name <- "D3"
drought_ths <- c(-1.5, -1.9)




# READ THRESHOLD TABLE

read_delim("~/bucket_mine/misc_data/CMIP5_model_temp_thresholds.csv") -> thresholds

thresholds %>% 
  select(1:6) %>% 
  pivot_longer(-Model, names_to = "Warm") %>% 
  mutate(Warm = str_sub(Warm, 3)) -> thresholds

thresholds %>% 
  mutate(Warm = ifelse(str_length(Warm) == 1, str_glue("{Warm}.0"), Warm)) -> thresholds





# LOOP THROUGH DOMAINS
walk(c("AFR", "AUS", "CAM", "CAS", "EAS", "EUR", "NAM", "SAM", "SEA", "WAS"), function(dom){      # ***************
  
  print(str_glue("********** PROCESSING DOM {dom} **********"))
  
  "~/bucket_mine/results/global_spei_ww/" %>%
    list.files(full.names = T) %>%
    .[str_detect(., dom)] %>%
    str_split("_", simplify = T) %>%
    .[, c(5,6)] %>%
    {str_glue("{.[,1]}_{.[,2]}")} %>%
    unique() -> mods
  
  mods[str_detect(mods, "CNRM|ICHEC", negate = T)] -> mods 
    
  # LOOP THROUGH MODELS
  mods %>% 
    walk(function(mod){
      
      print(str_glue("PROCESSING MODEL {mod}"))
      
      
      # LOOP THROUGH ACC PERIODS
      walk(c("03", "06", "12", "18"), function(acc){
        
        print(str_glue("   PROCESSING ACC {acc}"))
        
        # plan(sequential)
        # plan(multicore, workers = 10)
        
        
        # IMPORT DATA
        
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
          
          if(class(s) == "try-error") Sys.sleep(5)
          
        }
        
        toc()
        
        
        
        
        # plan(multicore, workers = 6)
        
        print(str_glue("      Processing 6 warming levels"))
        tic(str_glue("      -- Done"))
        
        
        # LOOP THROUGH WLs
        walk(c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0"), function(warm){
          
          
          print(str_glue("      Processing warming level {warm}"))
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
          
          fname <- str_glue("~/bucket_mine/results/global_spei_ww/derived/spei-{acc}_probability-{drought_name}_{dom}_{str_split(mod, '_', simplify = T)[,1]}_{mod_sh}_{warm}C.nc")
          
          
          
          s %>%
            filter(year(time) >= start_end[1],
                   year(time) <= start_end[2]) %>%
            
            st_apply(c(1,2), function(x){
              
              if(all(is.na(x))){
                NA
              } else {
                sum(x <= drought_ths[1] & x >= drought_ths[2], na.rm = T)/length(x)
              }
              
            },
            FUTURE = F,
            .fname = "prob") -> s_f
          
          func_write_nc_notime(s_f,
                               fname)
          
          
          
        }) # end of WLs loop
        
        toc()
        
        rm(s_int_raw, s_dur_raw)
        
      })
      
    })
  
})




