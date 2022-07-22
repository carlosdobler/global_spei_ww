
# source("~/00-mount.R")

source("scripts/00_setup.R")
source("scripts/penman_mine_remo.R")
source("scripts/write_nc.R")

library(lmomco)
list.files("scripts/functions_spei/", full.names = T) %>% 
  walk(source)

plan(multisession)

# Land raster to evaluate whether chunk should be calculated or skipped
st_read("~/bucket_mine/misc_data/ne_50m_land/ne_50m_land.shp") %>%
  mutate(a = 1) %>%
  select(a) %>%
  st_rasterize(st_as_stars(st_bbox(), dx = 0.2, dy = 0.2, values = NA)) -> land_rast


# DOMAIN LOOP
dom <- c("AFR", "AUS", "CAM", "CAS", "EAS", "EUR", "NAM", "SAM", "SEA", "WAS")[7]


# ********************************************
# STOP!
# Use down_and_regrid_cdo.R to download and 
# regrid files before proceeding
# ********************************************


vars <- c("hurs", "rsds", "sfcWind", "tasmax", "tasmin", "pr") %>% set_names()

# master table

str_glue("~/bucket_mine/remo/monthly/{dom}-22") %>% 
  list.dirs(recursive = F) %>% 
  .[str_detect(., str_flatten(vars, "|"))] %>% 
  map_dfr(function(d){
    
    pos_model <- 3
    pos_rmodel <- 6
    pos_date <- 9
    
    tibble(file = d %>%
             list.files() %>% 
             .[str_detect(., "regrid")] %>%
             .[str_detect(., "cut", negate = T)]) %>%
      mutate(
        
        var = file %>%
          str_split("_", simplify = T) %>%
          .[ , 1],
        
        model = file %>%
          str_split("_", simplify = T) %>%
          .[ , pos_model],
        
        rmodel = file %>%
          str_split("_", simplify = T) %>%
          .[ , pos_rmodel] %>% 
          str_split("-", simplify = T) %>% 
          .[ , 2],
        
        t_i = file %>%
          str_split("_", simplify = T) %>%
          .[ , pos_date] %>%
          str_sub(end = 6) %>% 
          str_c("01"),
        
        t_f = file %>%
          str_split("_", simplify = T) %>%
          .[ , pos_date] %>%
          str_sub(start = 8, end = 14) %>% 
          str_c("01")
        
      )
  }) %>% 
  mutate(model = str_glue("{rmodel}_{model}")) %>% 
  select(-rmodel) -> tb_files


# create chunks index table
source("scripts/tiling.R")

# orography
"~/bucket_mine/remo/fixed/" %>%
  list.files(full.names = T) %>%
  .[str_detect(., dom)] %>%
  .[str_detect(., "regrid")] %>% 
  .[str_detect(., "Had")] %>% 
  read_ncdf() %>%
  suppressMessages() %>%
  mutate(orog = set_units(orog, NULL)) %>%
  setNames("v") %>% 
  mutate(v = ifelse(v < 0, 0, v)) -> s_z

# TOA radiation from ERA

"~/bucket_mine/era/monthly/era5_monthly_mean_daily_radiation_shifted.nc" %>% 
  read_ncdf(var = "tisr", ncsub = cbind(start = c(1,1,1),
                                        count = c(NA, NA, 1))) -> s_e_proxy

which.min(abs(st_get_dimension_values(s_e_proxy, "longitude") - (st_bbox(s_proxy)[1]-1))) -> era_st_lon
which.min(abs(st_get_dimension_values(s_e_proxy, "longitude") - (st_bbox(s_proxy)[3]+1))) - era_st_lon -> era_ct_lon
which.min(abs(st_get_dimension_values(s_e_proxy, "latitude") - (st_bbox(s_proxy)[4]+1))) -> era_st_lat
which.min(abs(st_get_dimension_values(s_e_proxy, "latitude") - (st_bbox(s_proxy)[2]-1))) - era_st_lat -> era_ct_lat
  
# tic()
"~/bucket_mine/era/monthly/era5_monthly_mean_daily_radiation_shifted.nc" %>% 
  read_ncdf(var = "tisr", ncsub = cbind(start = c(era_st_lon, era_st_lat, 1),
                                        count = c(era_ct_lon, era_ct_lat, NA))) %>%
  suppressMessages() -> tisr_era
# toc()

st_dimensions(tisr_era)$longitude$offset <- st_get_dimension_values(s_e_proxy, "longitude")[era_st_lon]
st_dimensions(tisr_era)$latitude$offset <- st_get_dimension_values(s_e_proxy, "latitude")[era_st_lat]

tisr_era %>% 
  st_warp(s_proxy) %>%
  # st_warp(s_z) %>% 
  mutate(tisr = tisr %>% 
           set_units(MJ/m^2) %>% 
           set_units(NULL)) -> tisr_era

tisr_era %>% 
  st_apply(c(1,2), function(t){
    
    matrix(t, ncol = 12, byrow = T) %>% 
      apply(2, mean)
    
  },
  FUTURE = T,
  .fname = "month") %>% 
  aperm(c(2,3,1)) -> tisr_era_monthly_mean

rm(era_st_lon, era_st_lat, era_ct_lat, era_ct_lon)
rm(s_proxy, s_e_proxy, tisr_era)




# MODEL LOOP

walk(unique(tb_files$model)[-1], function(mod){                                                     # **********************************
  
  # mod <- unique(tb_files$model)[2]
  
  print(str_glue("PROCESSING MODEL {mod} ----------"))
  print(str_glue(" "))
  
  
  # MOVE NCs
  
  print(str_glue("   MOVING NC FILES"))
  
  dir_model_files <- str_glue("~/pers_disk/{mod}_wholefiles") # *****
  dir.create(dir_model_files)
  
  future_pwalk(tb_files %>% filter(model == mod), function(file, 
                                                           var, 
                                                           dom_ = dom, 
                                                           dir_ = dir_model_files,
                                                           ...){
    
    orig <- file %>%
      {str_glue("gs://clim_data_reg_useast1/remo/monthly/{dom_}-22/{var}/{.}")}
    
    dest <- file %>%
      {str_glue("{dir_}/{.}")}
    
    system(str_glue("gsutil cp {orig} {dest}"),
           ignore.stdout = TRUE, ignore.stderr = TRUE)
    
  })
  
  
  # TILE LOOP
  
  dir_tiles <- str_glue("~/pers_disk/{mod}_tiles") # *****
  dir.create(dir_tiles)
  
  pwalk(st_drop_geometry(chunks_ind), function(lon_ch, lat_ch, r){                                  # *****************************
    
    # r <- chunks_ind$r[1]
    # lon_ch <- chunks_ind$lon_ch[1]
    # lat_ch <- chunks_ind$lat_ch[1]
    
    print(str_glue("   PROCESSING TILE {r} / {nrow(chunks_ind)}"))
    
    
    # CROP FILES ----
    
    print(str_glue("      Cropping tile"))
    
    dir_tile_tmp <- str_glue("~/pers_disk/{mod}_tile_{r}") # *****
    dir.create(dir_tile_tmp)
    
    future_walk(list.files(dir_model_files, full.names = T), function(f,
                                                                      lon_chunks_ = lon_chunks,
                                                                      lat_chunks_ = lat_chunks,
                                                                      lon_ch_ = lon_ch,
                                                                      lat_ch_ = lat_ch,
                                                                      dir_tile_tmp_ = dir_tile_tmp){
      
      outfile <- f %>%
        str_split("/", simplify = T) %>% 
        .[, ncol(.)] %>% 
        str_sub(end = -4) %>%
        {str_glue("{dir_tile_tmp_}/{.}_cut.nc")}
      
      system(
        str_glue(
          "cdo 
          selindexbox,
          {lon_chunks_[[lon_ch_]][1]},
          {lon_chunks_[[lon_ch_]][2]},
          {lat_chunks_[[lat_ch_]][1]},
          {lat_chunks_[[lat_ch_]][2]} 
          {f} 
          {outfile}"
        ) %>% 
          str_flatten() %>% 
          str_replace_all("\n", ""),
        ignore.stderr = T
      )
      
    })
    
    
    # LOAD INTO STARS ----
    
    print(str_glue("      Importing/pre-processing vars"))
    
    map(vars, function(var_){
      
      # print(str_glue("         {var_}"))
      
      tb_files %>% 
        filter(model == mod,
               var == var_) %>% 
        
        future_pmap(function(file, t_i, t_f, dir_ = dir_tile_tmp, r_ = r, ...){
          
          seq(as_date(t_i), as_date(t_f), by = "1 month") -> d
          
          file %>% 
            str_sub(end = -4) %>%
            {str_glue("{.}_cut.nc")} %>% 
            {str_glue("{dir_}/{.}")} %>% 
            read_ncdf() %>% 
            suppressMessages() %>%
            st_set_dimensions("time", values = d)
          
        },
        .options = furrr_options(seed = NULL)) %>% 
        do.call(c, .) %>% 
        setNames("v") -> s
      
      s %>% 
        st_get_dimension_values("time") %>% 
        duplicated() %>% 
        which() -> dup
      
      if(length(dup) > 0){
        s %>% 
          slice("time", -dup) -> s
        
        st_get_dimension_values(s, "time") %>% 
          {seq(first(.), last(.), by = "1 month")} %>% 
          {st_set_dimensions(s, "time", .)} -> s
      }
      
      if(str_detect(var_, "tas")){
        
        s %>% 
          mutate(v = set_units(v, degC) %>% 
                   set_units(NULL)) -> s
        
      } else if(var_ == "pr"){
        
        s %>% 
          mutate(v = set_units(v, kg/m^2/d) %>%
                   set_units(NULL)) -> s
        
      } else if(var_ == "rsds"){
        
        s %>% 
          mutate(v = set_units(v, MJ/d/m^2) %>% 
                   set_units(NULL)) -> s
        
      } else {
        s %>% 
          mutate(v = set_units(v, NULL)) -> s
      }
      
      return(s)
      
    }) -> l_s_vars
    
    s_z %>%
      st_crop(l_s_vars[[1]]) %>%
      suppressMessages() -> l_s_vars[["orog"]]
    
    l_s_vars[["orog"]] %>% 
      st_dim_to_attr() %>% 
      select(2) %>% 
      setNames("v") -> l_s_vars[["lat"]]
    
    tisr_era_monthly_mean %>%
      st_crop(l_s_vars[[1]]) %>%
      suppressMessages() -> l_s_vars[["tisr"]]
    
    
    
    # CALCULATE PET (Penman-M) ----
    
    print(str_glue("      Calculating PET - Penman"))
    
    l_s_vars[c("tasmax",
               "tasmin",
               "rsds", 
               "tisr",
               "sfcWind", 
               "hurs", 
               "orog"#, 
               #"lat"
    )] %>% 
      map(pull, 1) -> s_vars_pet
    
    s_vars_pet %>% 
      {do.call(abind, c(., along = 3))} -> s_array
    
    names(dim(s_array)) <- c("lon", "lat", "time")
    
    s_array %>% 
      st_as_stars() -> s_array
    
    s_vars_pet %>% 
      map_int(~dim(.x)[3]) %>% 
      map_int(~ifelse(is.na(.x), 1L, .x)) %>% 
      unname() %>% 
      {c(1, .)} %>% 
      cumsum() -> index
    
    cbind(index[-length(index)],
          index[-1]-1) -> index
    
    l_s_vars[[1]] %>% st_get_dimension_values("time") %>% 
      {case_when(year(.) < 1980 ~ 331,
                 year(.) < 1990 ~ 345,
                 year(.) < 2000 ~ 360,
                 year(.) < 2010 ~ 379,
                 year(.) < 2020 ~ 402,
                 year(.) < 2030 ~ 431,
                 year(.) < 2040 ~ 468,
                 year(.) < 2050 ~ 513,
                 year(.) < 2060 ~ 570,
                 year(.) < 2070 ~ 639,
                 year(.) < 2080 ~ 717,
                 year(.) < 2090 ~ 801,
                 year(.) >= 2090 ~ 890)} -> cp
    
    first_month <- st_get_dimension_values(l_s_vars[[1]], "time") %>% first() %>% month()
    
    s_array %>% 
      st_apply(c(1,2), function(x, co22, f_m){
        
        penman_mine_remo(Tmax = x[index[1,1]:index[1,2]],
                         Tmin = x[index[2,1]:index[2,2]],
                         Rs = x[index[3,1]:index[3,2]],
                         Ra = x[index[4,1]:index[4,2]],
                         u2 = x[index[5,1]:index[5,2]],
                         RH = x[index[6,1]:index[6,2]],
                         z = x[index[7,1]:index[7,2]],
                         co2_ppm = co22,
                         first_month = f_m)
        
      },
      co22 = cp,
      f_m = first_month,
      .fname = "time",
      FUTURE = T) %>%
      
      aperm(c(2,3,1)) %>% 
      setNames("pet") -> s_pet_pm
    
    st_dimensions(s_pet_pm) <- st_dimensions(l_s_vars$pr)
    
    saveRDS(s_pet_pm, str_glue("{dir_tiles}/s_pet_pmco2_{r}.rds"))
    
    
    
    
    # CALCULATE PET (Th) ----
    
    print(str_glue("      Calculating PET - Thornwaite"))
    
    l_s_vars[c("tasmax",
             "tasmin",
             "lat"
    )] %>%
      map(pull, 1) -> s_vars_pet
    
    s_vars_pet %>%
      {do.call(abind, c(., along = 3))} -> s_array
    
    names(dim(s_array)) <- c("lon", "lat", "time")
    
    s_array %>%
      st_as_stars() -> s_array
    
    s_vars_pet %>%
      map_int(~dim(.x)[3]) %>%
      map_int(~ifelse(is.na(.x), 1L, .x)) %>%
      unname() %>%
      {c(1, .)} %>%
      cumsum() -> index
    
    cbind(index[-length(index)],
          index[-1]-1) -> index
    
    s_array %>%
      st_apply(c(1,2), function(x){
        
        if(anyNA(x)){
          rep(NA, index[1,2])
        } else {
          
          tas <- (x[index[1,1]:index[1,2]] + x[index[1,1]:index[1,2]]) / 2
          
          SPEI::thornthwaite(Tave = tas,
                             lat = x[index[3,1]:index[3,2]])
          
        }
        
      },
      .fname = "time",
      FUTURE = T) %>%
      
      aperm(c(2,3,1)) %>%
      setNames("pet") -> s_pet_th
    
    st_dimensions(s_pet_th) <- st_dimensions(l_s_vars$pr)
    
    saveRDS(s_pet_th, str_glue("{dir_tiles}/s_pet_th_{r}.rds"))
    
    
    
    # CALCULATE SPEIs ----
    
    # calculate spei
    print(str_glue("      Calculating many SPEIs"))
    
    l_s_vars[[1]] %>% 
      st_get_dimension_values("time") %>% 
      first() %>% 
      year() -> first_yr
    
    c(s_pet_pm,
      l_s_vars$pr) %>% 
      mutate(bal = v-pet) %>% 
      select(bal) -> s_bal
    
    walk(c(1,2,3,6,9,12,18,24,36), function(sc){
      
      # print(str_glue("         sc = {sc}"))
      
      s_bal %>% 
        st_apply(c(1,2), function(x){
          
          if(anyNA(x)){
            rep(NA, length(x))
          } else {
            
            # sc_1 <- sc - 1
            
            x %>% 
              ts(start = first_yr,
                 frequency = 12) -> x_ts
            
            spei(x_ts,
                 scale = sc,
                 ref.start = c(1971, 1),
                 ref.end = c(2000, 12)
            ) %>% 
              .$fitted %>% 
              as.vector() %>% 
              {ifelse(is.infinite(.), NA, .)} -> x_spei
            
            return(x_spei)
          }
        },
        .fname = "time",
        FUTURE = T) %>% 
        
        setNames("spei") %>% 
        st_set_dimensions("time", 
                          values = st_get_dimension_values(l_s_vars$pr, "time")) %>% 
        aperm(c(2,3,1)) -> s_spei
      
      str_pad(sc, 2, "left", "0") -> scc
      saveRDS(s_spei, str_glue("{dir_tiles}/s_spei-{scc}_{r}.rds"))
      
    })
    
    
    
    # CALCULATE PDSI ----
    print(str_glue("      Calculating PDSI"))
    
    l_s_vars[[1]] %>% 
      st_get_dimension_values("time") %>% 
      last() %>%
      year() -> last_yr
    
    which(seq(first_yr, last_yr) == 1971) -> cal_start_i
    which(seq(first_yr, last_yr) == 2000) -> cal_end_i
    
    c(l_s_vars$pr, s_pet_pm, along = 3) -> s_pr_pet
    dim(s_pr_pet)[3]/2 -> index
    
    s_pr_pet %>%
      st_apply(c(1,2), function(x){
        
        if(anyNA(x)){
          rep(NA, index)
        } else {
          
          x[1:index] %>%
            ts(start = first_yr,
               frequency = 12) -> x_P
          
          x[(index+1):(index*2)] %>%
            ts(start = first_yr,
               frequency = 12) -> x_PE
          
          scPDSI::pdsi(P = x_P,
                       PE = x_PE,
                       sc = T,
                       cal_start = cal_start_i,
                       cal_end = cal_end_i
          ) -> pdsi
          
          pdsi$X %>% as.vector() %>% na.omit()
          
        }
        
      },
      .fname = "time",
      FUTURE = T,
      future.seed = NULL) %>% 
      
      aperm(c(2,3,1)) %>%
      setNames("pdsi") %>%
      st_set_dimensions("time", 
                        st_get_dimension_values(l_s_vars$pr, "time")) -> s_pdsi
    
    saveRDS(s_pdsi, str_glue("{dir_tiles}/s_pdsi-01_{r}.rds"))
    
    
    
    # ****** 
    
    unlink(dir_tile_tmp, recursive = T)
    
    print(str_glue(" "))
    
  }) #end of tile loop
    
    
    
  
  # MOSAIC AND SAVE ----
  
  c("pet_pmco2", "pet_th",
    "spei-01", "spei-02", "spei-03", "spei-06", "spei-09", 
    "spei-12", "spei-18", "spei-24", "spei-36",
    "pdsi-01") %>%
    
    walk(function(fvar){
      
      print(str_glue("  MOSAICKING ({fvar})"))
      
      list.files(dir_tiles, full.names = T) %>%
        .[str_detect(., fvar)] %>% 
        map(readRDS) -> all_tiles
      
      seq_len(dim(all_tiles[[1]])[3]) %>%
        split(ceiling(./60)) %>%
        imap(function(t,i){

          if(as.numeric(i) %% 5 == 0){
            print(str_glue("      {as.numeric(i)*60} / {dim(all_tiles[[1]])[3]} mosaicked"))
          }

          all_tiles %>%
            map(slice, "time", t) -> all_tiles_sub

          all_tiles_sub %>%
            map(as, "SpatRaster") %>%
            do.call(terra::merge, .) %>%
            st_as_stars() %>%
            st_set_dimensions(3,
                              name = "time",
                              values = st_get_dimension_values(all_tiles_sub[[1]], "time"))

        }) -> s_mos
      
      do.call(c, c(s_mos, along = "time")) -> s_mos
      
      s_mos %>% 
        mutate(values = ifelse(is.infinite(values), NA, values)) -> s_mos
      
      fvar %>% 
        str_replace("_", "-") -> fvar
      
      fvar %>% 
        str_split("-", simplify = T) %>%
        .[,1] %>%
        {setNames(s_mos, .)} -> s_mos
      
      # save
      print(str_glue("  SAVING ({fvar})"))
      
      st_get_dimension_values(all_tiles[[1]], "time") %>% first() %>% as.character() %>% year() -> first_yr
      st_get_dimension_values(all_tiles[[1]], "time") %>% last() %>% as.character() %>% year() -> last_yr
      
      rm(all_tiles)
      
      if(str_detect(fvar, "spei|pdsi")){
        outfile <- str_glue("output/{dom}_{mod}_monthly_{fvar}_{first_yr}_{last_yr}_cal0p5.nc")
      } else {
        outfile <- str_glue("output/{dom}_{mod}_monthly_{fvar}_{first_yr}_{last_yr}.nc")
      }
      
      if(file.exists(outfile)){
        file.remove(outfile) %>% 
          invisible()
      }
      
      func_write_nc(s_mos, outfile)
      
      system(str_glue("gsutil cp {outfile} gs://clim_data_reg_useast1/results/global_spei_ww"))
      
      file.remove(outfile) %>% 
        invisible()
      
      rm(s_mos)
    
    }) # end of mosaic loop
  
  unlink(dir_tiles, recursive = T)
  unlink(dir_model_files, recursive = T)
  
  print(str_glue(" "))
  print(str_glue(" "))
  
}) # end of model loop

