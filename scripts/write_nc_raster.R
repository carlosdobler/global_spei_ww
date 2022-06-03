
func_write_nc <- function(raster_to_export, time_vector, var_name, file_name){
  
  # define dimensions
  dim_lon <- ncdf4::ncdim_def(name = "lon", 
                              units = "degrees_east", 
                              vals = raster_to_export %>% 
                                raster::extent() %>% 
                                {seq(.[1], .[2], by = 0.2) + 0.1} %>% 
                                .[-length(.)])
  
  dim_lat <- ncdf4::ncdim_def(name = "lat", 
                              units = "degrees_north", 
                              vals = raster_to_export %>% 
                                raster::extent() %>% 
                                {seq(.[4], .[3], by = -0.2) + 1} %>% 
                                .[-1])
  
  dim_time <- ncdf4::ncdim_def(name = "time", 
                               units = "days since 1970-01-01", 
                               vals = time_vector %>% 
                                 as.integer())
  
  # define variable
  vari <- ncdf4::ncvar_def(name = var_name,
                           units = "",
                           dim = list(dim_lon, dim_lat, dim_time), 
                           missval = -9999)
  
  # create file
  ncnew <- ncdf4::nc_create(filename = file_name, 
                            vars = list(vari),
                            force_v4 = T)
  
  # write data
  raster_to_export %>% 
    raster::values() -> a
  
  ncdf4::ncvar_put(nc = ncnew, 
                   varid = vari, 
                   vals = a, 
                   start = c(1,1,1), 
                   count = c(vari$varsize[1], vari$varsize[2], vari$varsize[3]))
  
  ncdf4::nc_close(ncnew)
  
}



