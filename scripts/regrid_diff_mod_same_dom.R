source("scripts/00_setup.R")

func_write_nc_notime <- function(star_to_export, file_name){
  
  # define dimensions
  dim_lon <- ncdf4::ncdim_def(name = "lon", 
                              units = "degrees_east", 
                              vals = star_to_export %>% st_get_dimension_values(1))
  
  dim_lat <- ncdf4::ncdim_def(name = "lat", 
                              units = "degrees_north", 
                              vals = star_to_export %>% st_get_dimension_values(2))
  
  # define variable
  vari <- ncdf4::ncvar_def(name = names(star_to_export),
                           units = "",
                           dim = list(dim_lon, dim_lat), 
                           missval = -9999)
  
  # create file
  ncnew <- ncdf4::nc_create(filename = file_name, 
                            vars = list(vari),
                            force_v4 = TRUE)
  
  # write data
  ncdf4::ncvar_put(nc = ncnew, 
                   varid = vari, 
                   vals = star_to_export %>% pull(1), 
                   start = c(1,1), 
                   count = c(star_to_export %>% st_get_dimension_values(1) %>% length(),
                             star_to_export %>% st_get_dimension_values(2) %>% length()))
  
  ncdf4::nc_close(ncnew)
  
}


doms <- c("AFR", "AUS", "CAM", "CAS", "EAS", "EUR", "NAM", "SAM", "SEA", "WAS")

doms %>% 
  set_names() %>% 
  map(function(dom){
    
    "~/bucket_mine/results/global_spei_ww/derived/" %>% 
      list.files(full.names = T) %>% 
      .[str_detect(., dom)] %>% 
      .[str_detect(., "spei-03")] %>% 
      .[str_detect(., "_mean_")] %>% 
      .[str_detect(., "_2.0C")] %>% 
      
      map(read_ncdf, proxy = T) %>% 
      map(st_dimensions)
    
  }) -> l


# DOMS to fix
# EAS: first 3 larger: make them smaller
# EUR: only 4 smaller (REMO Had)


# EAS:

"~/bucket_mine/results/global_spei_ww/derived/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "EAS")] %>% 
  .[str_detect(., "spei-01")] %>% 
  .[str_detect(., "probability")] %>% 
  .[str_detect(., "_2.0C")] %>% 
  .[4] %>% 
  read_ncdf() -> s1

"~/bucket_mine/results/global_spei_ww/derived/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "EAS")] %>% 
  .[str_detect(., "RegCM")] %>% #.[str_detect(., "regr")] %>% read_ncdf()
  
  future_walk(function(f){
    
    read_ncdf(f) %>% 
      suppressMessages() -> s
    
    s %>% 
      st_warp(s1) -> s
    
    # str_sub(f, end = -4) %>% 
    #   {str_glue("{.}_regr.nc")} -> outfile
    
    file.remove(f)
    
    func_write_nc_notime(s, f)
    
    
  })


# ****

"~/bucket_mine/results/global_spei_ww/derived/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "EAS")] %>% 
  .[str_detect(., "spei-03")] %>% 
  .[str_detect(., "_mean_")] %>% 
  .[str_detect(., "_2.0C")] %>% 
  .[4] %>% 
  read_ncdf() -> s1

"~/bucket_mine/results/global_spei_ww/derived/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "EAS")] %>% 
  .[str_detect(., "RegCM")] %>%
  .[str_detect(., "(_mean_|perc_)")] %>%
  
  future_walk(function(f){
    
    read_ncdf(f) %>% 
      suppressMessages() -> s
    
    s %>% 
      st_warp(s1) -> s
    
    # str_sub(f, end = -4) %>% 
    #   {str_glue("{.}_regr.nc")} -> outfile
    
    file.remove(f)
    
    func_write_nc_notime(s, f)
    
    
  })

# ******************************************

# EUR:

"~/bucket_mine/results/global_spei_ww/derived/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "EUR")] %>% 
  .[str_detect(., "spei-01")] %>% 
  .[str_detect(., "probability")] %>% 
  .[str_detect(., "_2.0C")] %>% 
  .[1] %>% 
  read_ncdf() -> s1

"~/bucket_mine/results/global_spei_ww/derived/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "EUR")] %>% 
  .[str_detect(., "REMO2015_Had")] %>% #.[str_detect(., "regr")] %>% read_ncdf()
  
  future_walk(function(f){
    
    read_ncdf(f) %>% 
      suppressMessages() -> s
    
    s %>% 
      st_warp(s1) -> s
    
    # str_sub(f, end = -4) %>% 
    #   {str_glue("{.}_regr.nc")} -> outfile
    
    file.remove(f)
    
    func_write_nc_notime(s, f)
    
    
  })


# *****

"~/bucket_mine/results/global_spei_ww/derived/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "EUR")] %>% 
  .[str_detect(., "spei-03")] %>% 
  .[str_detect(., "_mean_")] %>% 
  .[str_detect(., "_2.0C")] %>%
  .[1] %>% 
  read_ncdf() -> s1

"~/bucket_mine/results/global_spei_ww/derived/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "EUR")] %>% 
  .[str_detect(., "REMO2015_Had")] %>%
  .[str_detect(., "(_mean_|perc_)")] %>% 
  
  future_walk(function(f){
    
    read_ncdf(f) %>% 
      suppressMessages() -> s
    
    s %>% 
      st_warp(s1) -> s
    
    # str_sub(f, end = -4) %>% 
    #   {str_glue("{.}_regr.nc")} -> outfile
    
    file.remove(f)
    
    func_write_nc_notime(s, f)
    
    
  })



# ***************************

# FIX ORIGINALS

doms <- c("AFR", "AUS", "CAM", "CAS", "EAS", "EUR", "NAM", "SAM", "SEA", "WAS")

doms %>% 
  set_names() %>% 
  map(function(dom){
    
    "~/bucket_mine/results/global_spei_ww/" %>% 
      list.files(full.names = T) %>% 
      .[str_detect(., dom)] %>% 
      .[str_detect(., "spei-03")] %>%
      .[str_detect(., "(CNRM|EARTH)", negate = T)] %>%
      
      map(read_stars, proxy = T) %>% 
      map(st_dimensions)
    
  }) -> l



"~/bucket_mine/results/global_spei_ww/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., dom)] %>% 
  .[str_detect(., "spei-03")] %>%
  .[str_detect(., "(CNRM|EARTH)", negate = T)] %>%
  .[4] %>% 
  read_ncdf() -> s1


"~/bucket_mine/results/global_spei_ww/" %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., dom)] %>% 
  .[str_detect(., "RegCM")] %>% 
  # .[str_detect(., "Had")] %>%
  
  walk(function(f){
    
    print(f)
    
    f %>% read_ncdf() -> s
    
    st_warp(s, slice(s1, time, 5)) -> s
    
    func_write_nc_wtime(s, f)
    
  })



  
  