
dom <- c("AFR", "AUS", "CAM", "CAS", "EAS", "EUR", "NAM", "SAM", "SEA", "WAS")[6]


# source("~/00-mount.R")

source("scripts/v02/setup.R")
source("scripts/v02/penman_mine_remo.R")
source("scripts/v02/write_nc.R")

library(SPEI)
library(imputeTS)

plan(multicore)


vars <- c("hurs", "rsds", "sfcWind", "tasmax", "tasmin", "pr") %>% set_names()


# MASTER TABLE
{
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
  
  if(dom == "EUR"){
    tb_files %>% 
      filter(!model %in% c("RegCM4_CNRM-CERFACS-CNRM-CM5", "RegCM4_ICHEC-EC-EARTH")) -> tb_files
  }
  
  tb_files %>% 
    filter(year(as_date(t_i)) >= 1970) -> tb_files
  
}

# tb_files %>% group_by(model) %>% summarise(min(t_i))
# tb_files %>% group_by(model) %>% summarise(max(t_f))


# OROGRAPHY
"~/bucket_mine/remo/fixed/" %>%
  list.files(full.names = T) %>%
  .[str_detect(., dom)] %>%
  .[str_detect(., "regrid")] %>% 
  .[str_detect(., "Had")] %>%  # REMO
  read_ncdf() %>%
  suppressMessages() %>%
  mutate(orog = set_units(orog, NULL)) %>%
  setNames("v") %>% 
  mutate(v = ifelse(v < 0, 0, v)) -> s_z


# RADIATION (ERA5-based)
{
  # "~/bucket_mine/era/monthly/era5_monthly_mean_daily_radiation_shifted.nc" %>%
  #   read_ncdf(var = "tisr", proxy = F) -> tisr_era
  # 
  # tisr_era %>%
  #   mutate(tisr = tisr %>% 
  #            set_units(MJ/m^2) %>% 
  #            set_units(NULL)) -> tisr_era
  # 
  # tisr_era %>% 
  #   st_apply(c(1,2), function(t){
  #     
  #     matrix(t, ncol = 12, byrow = T) %>% 
  #       apply(2, mean)
  #     
  #   },
  #   FUTURE = T,
  #   .fname = "month") %>% 
  #   aperm(c(2,3,1)) -> tisr_era_monthly_mean
  # 
  # saveRDS(tisr_era_monthly_mean, "~/bucket_mine/misc_data/tisr_era_monthly_mean.rds")
}

readRDS("~/bucket_mine/misc_data/tisr_era_monthly_mean.rds") -> tisr_era_monthly_mean

if(dom == "AUS"){
  
  tisr_era_monthly_mean %>% 
    slice(longitude, 721:1440) -> tisr_1
  
  st_set_dimensions(tisr_1, 
                    which = "longitude", 
                    values = st_get_dimension_values(tisr_1, 
                                                     "longitude", 
                                                     center = F)) -> tisr_1
  
  st_set_crs(tisr_1, 4326) -> tisr_1
  
  tisr_era_monthly_mean %>% 
    slice(longitude, 1:720) -> tisr_2
  
  st_set_dimensions(tisr_2, 
                    which = "longitude", 
                    values = st_get_dimension_values(tisr_2, 
                                                     "longitude", 
                                                     center = F)+360) -> tisr_2
  
  st_set_crs(tisr_2, 4326) -> tisr_2
  
  list(tisr_1, tisr_2) %>% 
    map(as, "SpatRaster") %>% 
    do.call(terra::merge, .) %>% #plot()
    st_as_stars(proxy = F) -> tisr
  
  tisr %>% 
    setNames("tisr") -> tisr_era_monthly_mean
  
  rm(tisr, tisr_1, tisr_2)
  
}

# regrid
tisr_era_monthly_mean %>% 
  st_warp(s_z) -> tisr_era_monthly_mean


# REFERENCE GRID
str_glue("~/bucket_mine/remo/monthly/{dom}-22/") %>% 
  list.dirs(recursive = F) %>% 
  .[1] %>% 
  list.files(full.names = T) %>% 
  .[str_detect(., "regrid")] %>% 
  .[str_detect(., "cut", negate = T)] %>% 
  .[1] %>% 
  read_ncdf(ncsub = cbind(start = c(1,1,1),
                          count = c(NA,NA,1))) %>% 
  suppressMessages() %>% 
  adrop() %>% 
  mutate(a = 1) %>% 
  select(a) -> s_ref



# *****************************************************************************


# MODEL LOOP

for(mod in unique(tb_files$model)[-1]){                                                             # ******************
  
  print(str_glue(" "))
  print(str_glue("PROCESSING MODEL {mod} ----------"))
  tic(str_glue("Done with model {mod}"))
  
  
  
  # TILING ----
  # reference file for extent
  
  mod %>% 
    str_split("_", simplify = T) -> mod_split
  
  str_glue("~/bucket_mine/remo/monthly/{dom}-22/") %>% 
    list.dirs(recursive = F) %>% 
    .[1] %>% 
    list.files(full.names = T) %>% 
    .[str_detect(., "regrid")] %>% 
    .[str_detect(., "cut", negate = T)] %>%
    .[str_detect(., mod_split[,1])] %>%
    .[str_detect(., mod_split[,2])] %>%
    .[1] -> f
  
  source("scripts/v02/tiling.R")
  
  # Reference coords
  s_proxy %>%  
    {list(st_get_dimension_values(., "lon"),
          st_get_dimension_values(., "lat"))} -> coords
  
  
  
  
  # MOVE NCs ----
  {
    print(str_glue("   MOVING NC FILES"))
    
    dir_model_files <- str_glue("~/pers_disk/{mod}_wholefiles")
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
  }
  
  
  
  
  # TILES LOOP ----
  
  dir_tiles <- str_glue("~/pers_disk/{mod}_tiles") # *****
  dir.create(dir_tiles)
  
  pwalk(st_drop_geometry(chunks_ind), function(lon_ch, lat_ch, r, ...){
    
    # r <- chunks_ind$r[18]
    # lon_ch <- chunks_ind$lon_ch[18]
    # lat_ch <- chunks_ind$lat_ch[18]
    
    print(str_glue(" "))
    print(str_glue("PROCESSING TILE {r} / {nrow(chunks_ind)}"))
    tic(str_glue("DONE W/TILE {r} / {nrow(chunks_ind)}"))
    
    
    cbind(start = c(lon_chunks[[lon_ch]][1], lat_chunks[[lat_ch]][1], 1),
          count = c(lon_chunks[[lon_ch]][2] - lon_chunks[[lon_ch]][1]+1,
                    lat_chunks[[lat_ch]][2] - lat_chunks[[lat_ch]][1]+1,
                    NA)) -> ncs
    
    
    # IMPORT FILES ----
    {
      
      print(str_glue("      Importing vars"))
      tic("          -- everything loaded")
      map(vars, function(var_){  # future_?
        
        # import
        tic(str_glue("         {var_} done!"))
        
        tb_files %>%
          filter(model == mod,
                 var == var_) %>%
          
          future_pmap(function(file, t_i, t_f, dir_ = dir_model_files, r_ = r, ncs_ = ncs, ...){
            
            # print(file)
            
            file %>%
              {str_glue("{dir_}/{.}")} %>% 
              read_ncdf(ncsub = ncs_) %>%
              suppressMessages() -> s
            
            st_dimensions(s)$lon$offset <- coords[[1]][ncs[1,1]]
            st_dimensions(s)$lat$offset <- coords[[2]][ncs[2,1]]
            
            return(s)
            
          },
          .options = furrr_options(seed = NULL)) %>%
          do.call(c, .) %>% 
          setNames("v") -> s
        
        # fix duplicates and dates
        st_get_dimension_values(s, "time") %>% 
          as.character() %>% 
          as_date() -> d
        
        d %>%
          duplicated() %>% 
          which() -> dup
        
        if(length(dup) > 0){
          
          print(str_glue("  {var_} dupls: {d[dup]}"))
          
          s %>% 
            slice("time", -dup) %>% 
            suppressWarnings() -> s
          
          st_get_dimension_values(s, "time") %>% 
            as.character() %>% 
            as_date() -> d
          
        }
        
        s %>% 
          st_set_dimensions("time", values = d) -> s
        
        # change units
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
        
        toc()
        
        return(s)
        
      }) -> l_s_vars
      
      if(dom == "AUS" & all(st_get_dimension_values(l_s_vars[[1]], "lon") < 0)){
        
        l_s_vars %>% 
          map(function(s){
            
            s %>% 
              st_set_dimensions("lon", 
                                values = st_get_dimension_values(s, "lon", center = F)+360) %>% 
              st_set_crs(4326)
            
          }) -> l_s_vars
        
      }
      
      
      # Include additional vars
      s_z %>%
        st_warp(l_s_vars[[1]] %>% slice(time, 1)) %>%
        suppressMessages() -> l_s_vars[["orog"]]
      
      tisr_era_monthly_mean %>%
        st_warp(l_s_vars[[1]] %>% slice(time, 1)) %>%
        suppressMessages() -> l_s_vars[["tisr"]]
      
      toc()
    
    }
  
    
    # CALCULATE PET  ----
    {
      
      print(str_glue("      Calculating PET - Penman"))
      
      l_s_vars[c("tasmax",
                 "tasmin",
                 "rsds", 
                 "tisr",
                 "sfcWind", 
                 "hurs", 
                 "orog")] %>% 
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
        FUTURE = F) %>%
        
        aperm(c(2,3,1)) %>% 
        setNames("pet") -> s_pet_pm
      
      st_dimensions(s_pet_pm) <- st_dimensions(l_s_vars$pr)
      
      func_write_nc_wtime(s_pet_pm, str_glue("{dir_tiles}/s_pet-pmco2_{str_pad(r, 2, 'left', '0')}.nc"))
      
    }
    
    
    # CALCULATE SPEI ----
    {
      
      print(str_glue("      Calculating 3 SPEIs"))
      
      l_s_vars[[1]] %>%
        st_get_dimension_values("time") %>%
        first() %>%
        {c(year(.), month(.))} -> first_yr
      
      c(s_pet_pm,
        l_s_vars$pr) %>%
        mutate(bal = v-pet) %>%
        select(bal) -> s_bal
      
      
      walk(c(3,6,12), function(sc){
        
        print(str_glue("         sc = {sc}"))
        
        s_bal %>%
          st_apply(c(1,2), function(x){
            
            if(anyNA(x)){
              rep(NA, length(x))
            } else {
              
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
                {ifelse(is.infinite(.), NA, .)} %>%
                {.} -> x_spei
              
              c(x_spei[1:(sc-1)],
                na_interpolation(x_spei[-(1:(sc-1))])) -> x_spei
              
              return(x_spei)
            }
          },
          .fname = "time",
          FUTURE = T) %>%
          
          setNames("spei") -> s_spei
        
        s_spei %>% 
          st_set_dimensions("time",
                            values = st_get_dimension_values(l_s_vars$pr, "time")) %>%
          aperm(c(2,3,1)) -> s_spei
        
        func_write_nc_wtime(s_spei,
                            str_glue("{dir_tiles}/s_spei-{str_pad(sc, 2, 'left', '0')}_{str_pad(r, 2, 'left', '0')}.nc"))
        
        
      })
      
    }
    
    toc()
    
  })
  
  
  # MOSAIC ----
  
  
  # SET UP
  {
    dir_res <- str_glue("~/bucket_mine/results/global_spei_ww/new_raw/{dom}")
    if(isFALSE(dir.exists(dir_res))){
      dir.create(dir_res)  
    }
    
    
    dir_tiles %>% 
      list.files(full.names = T) %>% 
      .[1] %>% 
      read_stars(proxy = T) %>% 
      st_get_dimension_values("time") %>% 
      as_date() -> d
    
    d %>% 
      year() %>% 
      unique() -> d_yrs
    
    round(length(d_yrs)/30) -> n_lon
    
    split(d_yrs, 
          ceiling(seq_along(d_yrs)/(length(d_yrs)/n_lon))) -> yrs_chunks
    
    yrs_chunks %>% 
      map(function(y){
        
        d %>% 
          str_sub(end = 4) %>% 
          {. %in% y} %>% 
          which() %>% 
          {c(first(.), last(.))}
        
      }) -> d_pos
    
    
    unique(chunks_ind$lat_ch) %>% 
      sort() %>% 
      map(function(i){
        chunks_ind %>% 
          filter(lat_ch == i) %>% 
          pull(r) %>% 
          str_pad(2, "left", "0")
      }) -> tiles
  }
  
  
  # MOSAICKING
  {
    c("pet-pmco2", 
      "spei-03",
      "spei-06",
      "spei-12") %>% 
      
      walk(function(v){
        
        print(str_glue("      Mosaicking {v}"))
        tic(str_glue("      -- Done"))
        
        d_pos %>% 
          # iwalk(function(d_p, i){
          future_walk(function(d_p){
            
            # print(str_glue("         period {i} / {length(d_pos)}"))
            
            tiles %>% 
              map(function(r){
                
                dir_tiles %>%
                  list.files(full.names = T) %>%
                  .[str_detect(., v)] %>% 
                  .[str_detect(., str_flatten(str_glue("_{r}.nc"), "|"))] %>% 
                  map(read_ncdf,
                      proxy = F,
                      ncsub = cbind(start = c(1,1,d_p[1]),
                                    count = c(NA,NA,(d_p[2]-d_p[1]+1)))) %>% 
                  suppressMessages() %>% 
                  map(as, "SpatRaster") %>%
                  do.call(terra::merge, .)
                
              }) %>% 
              do.call(terra::merge, .) -> mos
            
            mos %>% 
              st_as_stars() %>% 
              st_set_dimensions(3, names = "time",
                                values = d[d_p[1]:d_p[2]]) %>% 
              st_warp(s_ref) -> mos
            
            d[d_p[1]] %>% str_remove_all("-") -> t_i
            d[d_p[2]] %>% str_remove_all("-") -> t_f
            
            func_write_nc_wtime(mos,
                                str_glue("{dir_res}/{v}_{dom}_{mod}_mon_{t_i}-{t_f}.nc"))
            
          },
          .options = furrr_options(seed = NULL)
          )
        
        toc()
        
      })
  }
  
  
  unlink(dir_model_files, recursive = T)
  unlink(dir_tiles, recursive = T)
  
  toc()
  
  plan(sequential)
  gc()
  plan(multicore)
  
} 

