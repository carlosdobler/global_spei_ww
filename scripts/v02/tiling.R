
# CREATE LAND LAYER

c(st_point(c(-180, -90)),
  st_point(c(180, 90))) %>%
  st_bbox() %>%
  st_set_crs(4326) -> box_reference

box_reference %>%
  st_as_stars(dx = 0.05, dy = 0.05, values = -9999) -> rast_reference_0.05

box_reference %>%
  st_as_stars(dx = 0.2, dy = 0.2, values = -9999) -> rast_reference_0.2

"~/bucket_mine/misc_data/ne_50m_land/ne_50m_land.shp" %>%
  st_read(quiet = T) %>%
  mutate(a = 1) %>%
  select(a) %>%
  st_rasterize(rast_reference_0.05) -> land

land %>%
  st_warp(rast_reference_0.2, use_gdal = T, method = "max") %>%
  suppressWarnings() %>% 
  setNames("a") %>%
  mutate(a = ifelse(a == -9999, NA, 1)) -> land

land %>%
  st_set_dimensions(c(1,2), names = c("lon", "lat")) -> land


if(dom == "AUS"){
  
  land %>% 
    slice(lon, 901:1800) -> land_1
  
  st_set_dimensions(land_1, which = "lon", values = st_get_dimension_values(land_1, "lon", center = F)) -> land_1
  
  st_set_crs(land_1, 4326) -> land_1
  
  land %>% 
    slice(lon, 1:900) -> land_2
  
  st_set_dimensions(land_2, which = "lon", values = st_get_dimension_values(land_2, "lon", center = F)+360) -> land_2
  
  st_set_crs(land_2, 4326) -> land_2
  
  # st_mosaic(land_1, land_2)
  
  list(land_1, land_2) %>% 
    map(as, "SpatRaster") %>% 
    do.call(terra::merge, .) %>%
    st_as_stars() -> land
  
  land %>% 
    setNames("a") -> land
  
  rm(land_1, land_2)
  
}



# *****************************************************************************


# OBTAIN TILES' LIMITS

# size of chunk (pixels in each dim)
sz <- 50

f %>% 
  read_ncdf(ncsub = cbind(start = c(1, 1, 1),
                          count = c(NA,NA,1))) %>% 
  suppressMessages() %>% 
  slice(time, 1) -> s_proxy

# lon: 

s_proxy %>% 
  dim() %>% 
  .[1] %>% 
  seq_len() -> d_lon

round(length(d_lon)/sz) -> n_lon

split(d_lon, 
      ceiling(seq_along(d_lon)/(length(d_lon)/n_lon))) %>% 
  map(~c(first(.x), last(.x))) -> lon_chunks

# lat:

s_proxy %>% 
  dim() %>% 
  .[2] %>% 
  seq_len() -> d_lat

round(length(d_lat)/sz) -> n_lat

split(d_lat, 
      ceiling(seq_along(d_lat)/(length(d_lat)/n_lat))) %>% 
  map(~c(first(.x), last(.x))) -> lat_chunks



# *****************************************************************************


# GENERATE TABLE W/ POLYGONS

imap(lon_chunks, function(lon_ch, lon_i){
  imap(lat_chunks, function(lat_ch, lat_i){
    
    s_proxy %>%
      slice(lon, lon_ch[1]:lon_ch[2]) %>%
      slice(lat, lat_ch[1]:lat_ch[2]) -> s_proxy_sub
    
    st_warp(land,
            s_proxy_sub) -> land_rast_sub
    
    s_proxy_sub %>%
      mutate(aa = 1) %>% 
      select(aa) %>% 
      st_as_sf(as_points = F, merge = T) %>%
      mutate(lon_ch = lon_i,
             lat_ch = lat_i) -> pol_tile
    
    pol_tile %>% 
      mutate(cover = ifelse(all(is.na(pull(land_rast_sub, 1))) | all(is.na(pull(s_proxy_sub, 1))), F, T)) %>% 
      select(-aa) -> pol_tile
    
    if(pol_tile$cover == T){
      land_rast_sub %>%
        st_as_sf() %>% 
        summarize() %>%
        suppressMessages() %>%
        mutate(lon_ch = lon_i,
               lat_ch = lat_i) -> pol_land
    } else {
      pol_land <- NULL
    }
    
    list(pol_tile, pol_land)
    
  })
  
}) %>% 
  do.call(c, .) -> pols


pols %>% 
  map_dfr(pluck, 1) -> tb_ref

tb_ref %>% 
  filter(cover == T) %>% 
  mutate(r = row_number()) -> chunks_ind

pols[tb_ref$cover %>% which()] %>% 
  map_dfr(pluck, 2) %>% 
  mutate(r = row_number()) -> chunks_ind_land



rm(f, #s_proxy, 
   d_lon, n_lon, d_lat, n_lat, sz,
   rast_reference_0.05, rast_reference_0.2, box_reference, land,
   pols)

