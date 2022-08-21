
func_write_nc_wtime <- function(star_to_export, file_name){
  
  # define dimensions
  dim_lon <- ncdf4::ncdim_def(name = "lon", 
                              units = "degrees_east", 
                              vals = star_to_export %>% st_get_dimension_values(1))
  
  dim_lat <- ncdf4::ncdim_def(name = "lat", 
                              units = "degrees_north", 
                              vals = star_to_export %>% st_get_dimension_values(2))
  
  dim_time <- ncdf4::ncdim_def(name = "time", 
                               units = "days since 1970-01-01", 
                               vals = star_to_export %>% 
                                 st_get_dimension_values(3) %>% 
                                 as.character() %>% 
                                 as_date() %>% 
                                 as.integer())
  
  # define variable
  vari <- ncdf4::ncvar_def(name = names(star_to_export),
                           units = "",
                           dim = list(dim_lon, dim_lat, dim_time), 
                           missval = -9999)
  
  # create file
  ncnew <- ncdf4::nc_create(filename = file_name, 
                            vars = list(vari),
                            force_v4 = TRUE)
  
  # write data
  ncdf4::ncvar_put(nc = ncnew, 
                   varid = vari, 
                   vals = star_to_export %>% pull(1), 
                   start = c(1,1,1), 
                   count = c(star_to_export %>% st_get_dimension_values(1) %>% length(),
                             star_to_export %>% st_get_dimension_values(2) %>% length(),
                             star_to_export %>% st_get_dimension_values(3) %>% length()))
  
  ncdf4::nc_close(ncnew)
  
}


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



