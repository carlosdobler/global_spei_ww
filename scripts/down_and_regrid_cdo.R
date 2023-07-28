

# str_glue("https://esg-dn1.nsc.liu.se/esg-search/wget?
#          domain={dom}-11&
#          experiment=rcp85&experiment=historical&
#          institute=ICTP&
#          time_frequency=mon&
# 
#          variable=hurs&
#          variable=pr&
#          variable=rsds&
#          variable=sfcWind&
#          variable=tasmax&
#          variable=tasmin&
# 
#          download_structure=domain,variable") %>%

str_glue("esgf-data.dkrz.de/esg-search/wget?
         domain={dom}-22&
         experiment=rcp85&experiment=historical&
         institute=GERICS&
         time_frequency=mon&

         variable=hurs&
         variable=pr&
         variable=rsds&
         variable=sfcWind&
         variable=tasmax&
         variable=tasmin&

         download_structure=domain,variable") %>%

  str_flatten() %>% 
  str_replace_all("\n", "") %>% 
  
  download.file(destfile = "~/bucket_mine/remo/monthly/down_script.sh",
                method = "wget")
  

# download in terminal with: 
# <bash down_script.sh -H>
# provide credentials


str_glue("~/bucket_mine/remo/monthly/{dom}-22/") %>%
  list.dirs(recursive = F) %>% 
  .[1] %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "regrid", negate = T)] %>% 
  .[str_detect(., "REMO")] %>% # REMO as reference
  .[1] -> ff

nc <- RNetCDF::open.nc(ff)

# RNetCDF::print.nc(nc)

x <- RNetCDF::var.get.nc(nc, "lon") #%>% ifelse(. > 180, .-360, .)
y <- RNetCDF::var.get.nc(nc, "lat")

# even values = center of grid cells
xfirst <- round(min(x), 1)
if(as.integer(round((xfirst - trunc(xfirst)) * 10)) %% 2 == 0) xfirst <- xfirst - 0.1

xlast <- round(max(x), 1)
if(as.integer(round((xlast - trunc(xlast)) * 10)) %% 2 == 0) xlast <- xlast - 0.1
# xlast <- 179.9

yfirst <- round(min(y), 1)
if(as.integer(round((yfirst - trunc(yfirst)) * 10)) %% 2 == 0) yfirst <- yfirst - 0.1

ylast <- round(max(y), 1)
if(as.integer(round((ylast - trunc(ylast)) * 10)) %% 2 == 0) ylast <- ylast - 0.1


c("gridtype = latlon",
  str_c("xfirst = ", xfirst),
  "xinc = 0.2",
  str_c("xsize = ", (xlast - xfirst)/0.2),
  str_c("yfirst = ", yfirst),
  "yinc = 0.2",
  str_c("ysize = ", (ylast - yfirst)/0.2)
) %>%
  write_lines("grid.txt")

# regrid with cdo:

str_glue("~/bucket_mine/remo/monthly/{dom}-22/") %>%
# str_glue("~/bucket_mine/remo/monthly/{dom}-11/") %>%  # ONLY EUR-11!
  list.dirs(recursive = F) %>%
  .[str_detect(., "hurs|pr|rsds|sfcWind|tasmax|tasmin")] %>%
  
  walk(function(dir_var){
    
    print(str_glue("PROCESSING {dir_var}"))
    
    dir_var %>% 
      list.files(full.names = T) %>% 
      # .[str_detect(., "RegCM4")] %>%                                                                 # **********************
      .[str_detect(., "regrid", negate = T)] %>% 
      
      future_walk(function(f){
        
        f %>%
          str_split("/") %>%
          unlist() %>%
          last() -> f_b
        
        f %>%
          # str_replace("11", "22") %>% # EUR-11 ONLY!
          # str_replace("11", "22") %>% # EUR-11 ONLY!
          str_remove(".nc") %>%
          str_c("_regrid.nc") -> f_out
        
        # tic(f_b)
        system(str_glue("cdo remapbil,grid.txt {f} {f_out}"),
               ignore.stdout = TRUE, ignore.stderr = TRUE
               )
        
        # file.remove(f) %>% 
        #   invisible()
        
        # toc()
        
      })
    
  })
  
 # *****
# orography:

"~/bucket_mine/remo/fixed/" %>%
  list.files(full.names = T) %>% 
  .[str_detect(., "orog")] %>% 
  .[str_detect(., dom)] %>% 
  .[str_detect(., "regrid", negate = T)] %>% 
  
  walk(function(f){
    
    f %>%
      str_split("/") %>%
      unlist() %>%
      last() -> f_b
    
    f %>%
      str_remove(".nc") %>%
      str_c("_regrid.nc") -> f_out
    
    tic(f_b)
    system(str_glue("cdo remapbil,grid.txt {f} {f_out}"),
           ignore.stdout = TRUE, ignore.stderr = TRUE)
    
    # file.remove(f) %>% 
    #   invisible()
    toc()
    
    
  })
