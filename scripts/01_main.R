# source("~/00-mount.R")

source("scripts/00_setup.R")
source("scripts/penman_mine.R")
source("scripts/write_nc.R")

plan(multicore, workers = availableCores() - 1)

# Land raster to evaluate whether chunk should be calculated
st_read("~/bucket_mine/misc_data/ne_50m_land/ne_50m_land.shp") %>%
  mutate(a = 1) %>%
  select(a) %>%
  st_rasterize(st_as_stars(st_bbox(), dx = 0.2, dy = 0.2, values = NA)) -> land_rast


# DOMAIN LOOP
dom <- c("AFR", "AUS", "CAM", "CAS", "EAS", "EUR", "NAM", "SAM", "SEA", "WAS")[6]


# ********************************************
# STOP!
# Use down_and_regrid_cdo.R to download and 
# regrid files before proceeding
# ********************************************


# master tables
str_glue("~/bucket_mine/remo/monthly/{dom}-22") %>% 
  list.dirs(recursive = F) %>% 
  map_dfr(function(d){
    
    pos_model <- 3
    pos_date <- 9
    
    
    tibble(file = d %>%
             list.files()) %>%
      mutate(
        
        var = file %>%
          str_split("_", simplify = T) %>%
          .[ , 1],
        
        model = file %>%
          str_split("_", simplify = T) %>%
          .[ , pos_model],
        
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
  }) -> tb_files

# create chunks index table
source("scripts/tiling.R")


# MODEL LOOP
for(mod in c("Had", "MPI", "Nor")){                                                                  # ****************************
  
  print(str_glue("PROCESSING MODEL {mod} ----------"))
  print(str_glue(" "))
  
  dir_chunks_dom <- str_glue("~/pers_disk/{dom}_{mod}")
  dir.create(dir_chunks_dom)
  
  # case_when(mod == "Had" ~ 2014,
  #           mod == "MPI" ~ 2006,
  #           mod == "Nor" ~ 2019) -> mid_yr
  
  # orography
  "~/bucket_mine/remo/fixed/" %>%
    list.files(full.names = T) %>%
    .[str_detect(., dom)] %>%
    .[str_detect(., "regrid")] %>% 
    .[str_detect(., "Had")] %>% 
    read_ncdf() %>%
    suppressMessages() %>%
    mutate(orog = set_units(orog, NULL)) %>%
    setNames("v") -> s_z
  
  # str_glue("~/bucket_risk/RCM_regridded_data/REMO2015/{dom}/daily/model_attributes/") %>%
  #   list.files(full.names = T) %>%
  #   .[str_detect(., "orog")] %>%
  #   .[str_detect(., mod)] %>%
  #   .[1] %>% 
  #   read_ncdf() %>%
  #   suppressMessages() %>%
  #   mutate(orog = set_units(orog, NULL)) %>%
  #   setNames("v") -> s_z
  # 
  # st_set_dimensions(s_z, "lon", values = round(st_get_dimension_values(s_z, "lon"), 1)) -> s_z
  # st_set_dimensions(s_z, "lat", values = round(st_get_dimension_values(s_z, "lat"), 1)) -> s_z
  # st_set_crs(s_z, 4326) -> s_z
  
  # TOA radiation from ERA
  "~/bucket_mine/era/monthly/era5_monthly_mean_daily_radiation_shifted.nc" %>% 
    read_ncdf(var = "tisr") %>% 
    suppressMessages() %>%
    st_warp(s_z) %>% 
    mutate(tisr = tisr %>% 
             set_units(MJ/m^2) %>% 
             set_units(NULL)) -> tisr_era
  
  tisr_era %>% 
    st_apply(c(1,2), function(t){
      
      tibble(m = rep(1:12, 42),
             t = t) %>% 
        group_by(m) %>% 
        summarize(t = mean(t)) %>% 
        pull(t)
      
    },
    FUTURE = T,
    .fname = "month") %>% 
    aperm(c(2,3,1)) -> tisr_era_monthly_mean
  
  
  # CHUNKS LOOP
  tb_files %>% 
    filter(str_detect(model, mod)) %>% 
    mutate(rr = row_number()) -> tb_files_sub
  
  pwalk(chunks_ind[16:35,], function(lon_ch, lat_ch, r, ...){                                         ##################
    
    # r <- chunks_ind$r[1]
    # lon_ch <- chunks_ind$lon_ch[1]
    # lat_ch <- chunks_ind$lat_ch[1]
    
    print(str_glue("  PROCESSING CHUNK {r} / {nrow(chunks_ind)}"))
    
    # clip
    tic(str_glue("   Done clipping"))
    pwalk(tb_files_sub, function(file, var, rr, ...){

      infile <- str_glue("~/bucket_mine/remo/monthly/{dom}-22/{var}/{file}")

      outfile <- file %>%
        str_sub(end = -4) %>%
        {str_glue("~/bucket_mine/remo/monthly/{dom}-22/{var}/{.}_cut.nc")}
      
      # tic(str_glue("{rr} / {nrow(tb_files_sub)}"))
      system(
        str_glue(
          "cdo 
          selindexbox,
          {lon_chunks[[lon_ch]][1]},
          {lon_chunks[[lon_ch]][2]},
          {lat_chunks[[lat_ch]][1]},
          {lat_chunks[[lat_ch]][2]} 
          {infile} 
          {outfile}"
        ) %>% 
          str_flatten() %>% 
          str_replace_all("\n", ""),
        ignore.stderr = T
      )
      # toc()

    })
    toc()


    # move
    dir_chunks_tmp <- str_glue("~/pers_disk/{dom}_{mod}_{r}")
    dir.create(dir_chunks_tmp)

    tic(str_glue("   Done moving/deleting"))
    pwalk(tb_files_sub, function(file, var, rr, ...){

      orig <- file %>%
        str_sub(end = -4) %>%
        {str_glue("gs://clim_data_reg_useast1/remo/monthly/{dom}-22/{var}/{.}_cut.nc")}

      dest <- file %>%
        str_sub(end = -4) %>%
        {str_glue("{dir_chunks_tmp}/{.}_cut.nc")}

      # tic(str_glue("{rr} / {nrow(tb_files_sub)}"))

      system(str_glue("gsutil -m cp {orig} {dest}"),
             ignore.stdout = TRUE, ignore.stderr = TRUE)

      # delete files
      # access denied:
      # system(str_glue("gsutil rm {orig}"),
      #        ignore.stdout = TRUE, ignore.stderr = TRUE)

      # alternative
      file %>%
        str_sub(end = -4) %>%
        {str_glue("~/bucket_mine/remo/monthly/{dom}-22/{var}/{.}_cut.nc")} %>%
        file.remove() %>%
        invisible()

      # toc()

    })
    toc()
    
    
    # load vars
    tic(str_glue("   Done loading/deriving vars"))
    
    vars <- c("hurs", "rsds", "sfcWind", "tas", "pr") %>% set_names()
    map(vars, function(var){
      
      # print(var)
      
      dir_chunks_tmp %>% 
        list.files(full.names = T) %>% 
        .[str_detect(., var)] %>%
        map(read_ncdf) %>%  
        suppressMessages() %>% 
        map(~st_set_dimensions(.x, "time",
                               values = st_get_dimension_values(.x, "time") %>% 
                                 as.character() %>% 
                                 as_date())) %>%
        do.call(c, .) %>% 
        setNames("v") -> s
      
      if(var == "tas"){
        
        s %>% 
          mutate(v = set_units(v, degC) %>% 
                   set_units(NULL)) -> s
        
      } else if(var == "pr"){
        
        if(mod == "Had"){
          
          s %>% 
            mutate(v = set_units(v, kg/m^2/d) %>%
                     set_units(NULL) %>% 
                     {.*30}) -> s
          
        } else {
          
          s %>% 
            st_get_dimension_values("time") %>%
            days_in_month() %>% 
            unname() -> days_mth
          
          s %>% 
            mutate(v = set_units(v, kg/m^2/d) %>%
                     set_units(NULL)) %>% 
            st_apply(c(1,2),
                     function(x) x * days_mth,
                     .fname = "time") %>%
            st_set_dimensions("time", values = st_get_dimension_values(s, "time")) %>%
            aperm(c(2,3,1)) -> s
          
        }
        
      } else if(var == "rsds"){
        
        s %>% 
          mutate(v = set_units(v, MJ/d/m^2) %>% 
                   set_units(NULL)) -> s
        
      } else {
        s %>% 
          mutate(v = set_units(v, NULL)) -> s
      }
      
      return(s)
      
    }) -> s_vars
    
    s_vars[[1]] %>% 
      st_get_dimension_values("time") -> dates
    
    # derive additional vars
    c(s_vars$hurs,
      s_vars$tas) %>% 
      setNames(c("hurs", "tas")) %>% 
      mutate(dewp = 243.04*(log(hurs/100)+((17.625*tas)/(243.04+tas)))/
               (17.625-log(hurs/100)-((17.625*tas)/(243.04+tas)))) %>% 
      select(dewp) %>% 
      setNames("v") -> s_vars[["dewp"]]
    
    # s_z %>% 
    #   filter(lon >= st_get_dimension_values(s_vars[[1]], "lon", where = "start")[1],
    #          lon <= st_get_dimension_values(s_vars[[1]], "lon", where = "end") %>% last(),
    #          lat >= st_get_dimension_values(s_vars[[1]], "lat", where = "start")[1],
    #          lat <= st_get_dimension_values(s_vars[[1]], "lat", where = "end") %>% last()) -> s_vars[["orog"]]
    
    s_z %>%
      st_crop(s_vars[[1]]) %>%
      suppressMessages() -> s_vars[["orog"]]
    
    s_vars[["orog"]] %>% 
      st_dim_to_attr() %>% 
      select(2) %>% 
      setNames("v") -> s_vars[["lat"]]
    
    # if(dim(s_vars[["orog"]])[2] != dim(s_vars[[1]])[2]){
    #   s_vars[1:6] %>% 
    #     map(st_crop, s_vars[["orog"]]) %>% 
    #     suppressMessages() -> s_vars[1:6]
    # }
    
    # tisr_era_monthly_mean %>% 
    #   filter(lon >= st_get_dimension_values(s_vars[[1]], "lon", where = "start")[1],
    #          lon <= st_get_dimension_values(s_vars[[1]], "lon", where = "end") %>% last(),
    #          lat >= st_get_dimension_values(s_vars[[1]], "lat", where = "start")[1],
    #          lat <= st_get_dimension_values(s_vars[[1]], "lat", where = "end") %>% last()) -> s_vars[["tisr"]]
    
    tisr_era_monthly_mean %>%
      st_crop(s_vars[[1]]) %>%
      suppressMessages() -> s_vars[["tisr"]]
    
    toc()
    
    
    # *************
    
    # calculate pet (penman)
    tic(str_glue("   Done calculating PET PM"))
    
    s_vars[c("tas", 
            "rsds", 
            "tisr",
            "sfcWind", 
            "dewp", 
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
    
    dates %>% 
      days_in_month() %>% 
      unname() -> dm
    
    case_when(year(dates) < 1980 ~ 331,
              year(dates) < 1990 ~ 345,
              year(dates) < 2000 ~ 360,
              year(dates) < 2010 ~ 379,
              year(dates) < 2020 ~ 402,
              year(dates) < 2030 ~ 431,
              year(dates) < 2040 ~ 468,
              year(dates) < 2050 ~ 513,
              year(dates) < 2060 ~ 570,
              year(dates) < 2070 ~ 639,
              year(dates) < 2080 ~ 717,
              year(dates) < 2090 ~ 801,
              year(dates) >= 2090 ~ 890) -> cp
    
    s_array %>% 
      st_apply(c(1,2), function(x, days_mth, co2_ppm){
        
        penman_mine(Tmean = x[index[1,1]:index[1,2]],
                    Rs = x[index[2,1]:index[2,2]],
                    Ra = x[index[3,1]:index[3,2]],
                    u2 = x[index[4,1]:index[4,2]],
                    Tdew = x[index[5,1]:index[5,2]],
                    z = x[index[6,1]:index[6,2]],
                    #lat = x[index[7,1]:index[7,2]],
                    co2adj = T,
                    model_i = mod,
                    days_mth = days_mth,
                    co2_ppm = co2_ppm)
        
      },
      days_mth = dm,
      co2_ppm = cp,
      .fname = "time",
      FUTURE = T) %>%
      
      aperm(c(2,3,1)) %>% 
      setNames("pet") -> s_pet_pm
    
    st_dimensions(s_pet_pm) <- st_dimensions(s_vars$pr)
    
    saveRDS(s_pet_pm, str_glue("{dir_chunks_dom}/s_pet_pmco2_{dom}_{mod}_{r}.rds"))
    
    toc()
    
    
    # *************
    
    # calculate pet with thornwaite
    tic(str_glue("   Done calculating PET Th"))
    
    s_vars[c("tas", 
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
          rep(NA, length(dates))
        } else {
          
          SPEI::thornthwaite(Tave = x[index[1,1]:index[1,2]],
                             lat = x[index[2,1]:index[2,2]])
          
        }
        
      },
      .fname = "time",
      FUTURE = T) %>%
      
      aperm(c(2,3,1)) %>% 
      setNames("pet") -> s_pet_th
    
    st_dimensions(s_pet_th) <- st_dimensions(s_vars$pr)
    
    saveRDS(s_pet_th, str_glue("{dir_chunks_dom}/s_pet_th_{dom}_{mod}_{r}.rds"))
    
    toc()
    
    
    # *************
    
    # calculate spei
    tic(str_glue("   Done calculating 7 SPEIs"))
    
    dates %>%
      first() %>% 
      year() -> first_yr
    
    c(s_pet_pm,
      s_vars$pr) %>% 
      mutate(bal = v-pet) %>% 
      select(bal) -> s_bal
    
    walk(c(1,3,6,9,12,24,36), function(sc){
      
      s_bal %>% 
        st_apply(c(1,2), function(x){
          
          if(anyNA(x)){
            rep(NA, length(dates))
          } else {
            
            sc_1 <- sc - 1
            
            x %>% 
              ts(start = first_yr,
                 frequency = 12) -> x_ts
            
            SPEI::spei(x_ts,
                       scale = sc,
                       # ref.start = c(mid_yr-10, 1),
                       # ref.end = c(mid_yr+10, 12)
                       ref.start = c(1971, 1),
                       ref.end = c(2000, 12)
            ) %>% 
              .$fitted %>% 
              as.vector() %>% 
              {ifelse(is.infinite(.), NA, .)} -> x_spei
            
            if(sc_1 == 0){
              
              x_spei %>% 
                imputeTS::na_interpolation("spline", maxgap = 5)
              
            } else {
              
              x_spei %>% 
                .[-(1:sc_1)] %>% 
                imputeTS::na_interpolation("spline", maxgap = 5) %>% 
                {c(rep(NA, sc_1), .)}
              
            }
          }
        },
        .fname = "time",
        FUTURE = T) %>% 
        
        setNames("spei") %>% 
        st_set_dimensions("time", values = dates) %>% 
        aperm(c(2,3,1)) -> s_spei
      
      str_pad(sc, 2, "left", "0") -> scc
      saveRDS(s_spei, str_glue("{dir_chunks_dom}/s_spei-{scc}_{dom}_{mod}_{r}.rds"))
      
    })
    
    toc()
    
    
    # *************
    
    # calculate pdsi
    tic(str_glue("   Done calculating 2 PDSIs"))
    
    dates %>%
      last() %>%
      year() -> last_yr
    
    which(seq(first_yr, last_yr) == 1971) -> cal_start_i
    which(seq(first_yr, last_yr) == 2000) -> cal_end_i
    
    c(s_vars$pr, s_pet_pm, along = 3) -> s_pr_pet 
    dim(s_pr_pet)[3]/2 -> index
    
    
    walk(c(0, 6), function(mw){
      
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
            )$X %>% as.vector() -> ts_pdsi
            
            if(mw != 0){
              zoo::rollmean(ts_pdsi,
                            k = mw,
                            align = "right",
                            fill = NA)
            } else {
              ts_pdsi
            }
            
          }
          
        },
        .fname = "time",
        FUTURE = T,
        future.seed = NULL) %>%
        
        aperm(c(2,3,1)) %>% 
        setNames("pdsi") %>% 
        st_set_dimensions("time", values = dates) -> s_pdsi
      
      str_pad(mw, 2, "left", "0") -> mww
      saveRDS(s_pdsi, str_glue("{dir_chunks_dom}/s_pdsi-{mww}_{dom}_{mod}_{r}.rds"))
      
    })
    
    toc()
    
    unlink(dir_chunks_tmp, recursive = T)
    
    print(str_glue(" "))
    
  })
  
  
  
  

  c("pet_pmco2", "pet_th", 
    "spei-01", "spei-03", "spei-06", "spei-09", "spei-12", "spei-24", "spei-36",
    "pdsi-00", "pdsi-06") %>%                                                                       # ********************
    .[-1] %>% 
    walk(function(fvar){
      
      # mosaick
      print(str_glue("  MOSAICKING ({fvar})"))
      
      list.files(dir_chunks_dom, full.names = T) %>%
        .[str_detect(., fvar)] %>% 
        map(readRDS) -> chunks
      
      seq_len(dim(chunks[[1]])[3]) %>% 
        split(ceiling(./60)) %>%
        imap(function(t,i){
          
          if(as.numeric(i) %% 5 == 0){
            print(str_glue("      {i} mosaicked"))
          }
          # print(i)
          
          chunks %>% 
            map(slice, "time", t) -> chunks_sub 
          
          chunks_sub %>%   
            map(as, "SpatRaster") %>% 
            do.call(terra::merge, .) %>% 
            st_as_stars() %>% 
            st_set_dimensions(3, 
                              name = "time",
                              values = st_get_dimension_values(chunks_sub[[1]], "time"))
          
        }) -> s_mos
      
      do.call(c, c(s_mos, along = "time")) -> s_mos
      
      fvar %>% 
        str_replace("_", "-") -> fvar
      
      fvar %>% 
        str_split("-", simplify = T) %>%
        .[,1] %>%
        {setNames(s_mos, .)} -> s_mos
      
      # save
      print(str_glue("  SAVING ({fvar})"))
      
      st_get_dimension_values(chunks[[1]], "time") %>% first() %>% as.character() %>% year() -> first_yr
      st_get_dimension_values(chunks[[1]], "time") %>% last() %>% as.character() %>% year() -> last_yr
      
      rm(chunks)
      
      
      if(str_detect(fvar, "spei|pdsi")){
        outfile <- str_glue("~/bucket_mine/results/global_spei_ww/{dom}_{mod}_monthly_{fvar}_{first_yr}_{last_yr}_cal0p5.nc")
      } else {
        outfile <- str_glue("~/bucket_mine/results/global_spei_ww/{dom}_{mod}_monthly_{fvar}_{first_yr}_{last_yr}.nc")
      }
      
      func_write_nc(s_mos, outfile)
      
    })
  
  unlink(dir_chunks_dom, recursive = T)
  
  print(str_glue(" "))
  print(str_glue(" "))
  
}

























# MODEL LOOP
for(mod in c("Had", "MPI", "Nor")){
  
  print(str_glue("[{dom}] [{mod}] STARTED"))
  tic(str_glue("      Done with model {mod} of domain {dom}")) # 1
  
  # number of years
  if(mod == "Had"){
    yrs <- 130
  } else {
    yrs <- 131
  }
  
  
  # DOWNLOAD FILES ******
  
  print(str_glue(" "))
  print(str_glue("[{dom}] [{mod}] Downloading files"))
  tic(str_glue("   Done downloading")) # 3
  
  dir_down <- str_glue("~/pers_disk/{dom}") # create download directory
  dir.create(dir_down)
  source("scripts/02_download.R")
  
  toc() # 3
  
  
  # CREATE DATES VECTOR *****
  
  if(mod == "Had"){
    as.PCICt("1970-01-01", cal = "360") + (seq(0, yrs*360-1) * (60*60*24)) -> dates
  } else {
    seq(as_date("19700101"), as_date("21001231"), by = "1 day") %>% 
      as_datetime() %>% as.PCICt(cal = "gregorian") -> dates
  }
  
  
  # CREATE TILES INDICES *****
  
  source("scripts/03_tiling.R")
  
  
  # CALCULATE INDICES *****
  
  case_when(mod == "Had" ~ 2014,
            mod == "MPI" ~ 2006,
            mod == "Nor" ~ 2019) -> mid_yr
  
  # loop across chunks
  dir_tmp_chunks <- "~/pers_disk/tmp_chunks"
  dir.create(dir_tmp_chunks)
  
  pwalk(chunks_ind, function(lon_ch, lat_ch, r, ...){
    
    print(str_glue(" "))
    print(str_glue("[{dom}] [{mod}] Processing chunk {r} / {nrow(chunks_ind)}"))
    tic(str_glue("      Done processing chunk")) # 4
    
    # import all variables
    
    dir_tmp_vars <- "~/pers_disk/tmp_vars"
    dir.create(dir_tmp_vars)
    
    walk(c("tasmax", "tasmin", "pr"), function(v){
      
      # dir_down %>% 
      #   list.files(full.names = T) %>% 
      #   .[str_detect(., v)] -> files_vector
      
      tb_files %>%
        map_dfr(~.x) %>%
        filter(str_detect(file, v)) %>%
        filter(str_detect(file, mod)) %>% 
        arrange(t_i) %>% 
        pull(file) %>% 
        {str_c(dir_down,"/", .)} -> files_vector
      
      imap(files_vector, function(f, i){
        
        tic(str_glue("   Imported {v} - {i} / {length(files_vector)}")) # 5
        
        read_ncdf(f, ncsub = cbind(start = c(lon_chunks[[lon_ch]][1],
                                             lat_chunks[[lat_ch]][1],
                                             1),
                                   count = c(lon_chunks[[lon_ch]][2] - lon_chunks[[lon_ch]][1] +1,
                                             lat_chunks[[lat_ch]][2] - lat_chunks[[lat_ch]][1] +1,
                                             NA))) %>%
          suppressMessages() -> s_imp
        
        toc() # 5
        
        return(s_imp)
        
      }) -> s
      
      s %>%
        do.call(c, .) %>%
        setNames("var") -> s
      
      if(str_detect(v, "tas")){
        s %>%
          mutate(var = var %>% set_units(degC)) -> s
      } else {
        s %>%
          mutate(var = var %>% set_units(kg/m^2/d)) -> s
      }
      
      st_set_dimensions(s, "time", values = dates) -> s
      
      print(str_glue("      Saving"))
      tic(str_glue("      Done saving"))
      saveRDS(s, str_glue("{dir_tmp_vars}/tmp_star_{v}.rds"))
      toc()
      
    })
    
    # stack three variables along time
    list.files(dir_tmp_vars, full.names = T) %>%
      {c(.[str_detect(., "tasmax")], .[str_detect(., "tasmin")], .[str_detect(., "pr")])} %>% 
      map(readRDS) -> s
    
    unlink(dir_tmp_vars, recursive = T)
    
    s %>% 
      map(mutate, var = var %>% set_units(NULL)) %>% 
      {do.call(c, c(., along = 3))} -> s_stack
    
    # add lat to determine northern hemisphere
    s[[1]] %>%
      filter(time == as_date("1970-01-01")) %>%
      st_dim_to_attr() %>%
      dplyr::select("lat") %>% 
      {c(s_stack, ., along = 3)} -> s_stack
    
    
    # *****
    
    # calculate ETCCDI indices
    
    print(str_glue(" "))
    print(str_glue("   Calculating etccdi"))
    tic(str_glue("   Done with etccdi")) # 6.1
    
    cbind(seq(1, length(dates)*3, length(dates)),
          seq(1, length(dates)*3, length(dates)) + length(dates)-1) -> ind
    
    func_ecctdi(s_stack, ind, dates) -> s_etccdi
    
    # save ETCCDI indices
    
    print(str_glue("   Saving etccdi"))
    
    walk2(seq(1, yrs*27, yrs), 
          c("fd", "su", "id", "tr",
            "gsl",
            "txx", "tnx", "txn", "tnn",
            "tn10p", "tx10p", "tn90p", "tx90p",
            "wsdi", "csdi", "dtr",
            "rx1day", "rx5day",
            "sdii", "r10mm", "r20mm", "rnnmm",
            "cdd", "cwd",
            "r95ptot", "r99ptot", "prcptot"),
          
          function(t, i){
            
            s_etccdi %>% 
              slice(time, t:(t+yrs-1)) %>% 
              st_set_dimensions("time", values = dates %>% .[month(.) == 1 & day(.) == 1]) %>% 
              setNames(i) %>% 
              
              saveRDS(str_glue("{dir_tmp_chunks}/star_{i}_{lon_ch}_{lat_ch}.rds"))
            
          })
    
    toc() # 6.1
    
    
    # *****
    
    # calculate biovars
    
    print(str_glue("   Calculating biovars"))
    tic(str_glue("   Done with biovars")) # 6.2
    
    func_biovars(s_stack, ind, dates) -> s_biovar
    
    # save biovars
    
    print(str_glue("   Saving biovars"))
    
    walk2(seq(1, yrs*19, yrs), 
          str_glue("biovar{seq(1,19) %>% str_pad(2, 'left', '0')}"),
          
          function(t, i){
            
            s_biovar %>% 
              slice(time, t:(t+yrs-1)) %>% 
              st_set_dimensions("time", values = dates %>% .[month(.) == 1 & day(.) == 1]) %>% 
              setNames(i) %>% 
              
              saveRDS(str_glue("{dir_tmp_chunks}/star_{i}_{lon_ch}_{lat_ch}.rds"))
            
          })
    
    toc() # 6.2
    
    print(str_glue(" "))
    
    toc() # 4
    
    print(str_glue(" "))
    
  }) # end of chunk loop
  
  
  # MOSAICK *****
  
  print(str_glue(" "))
  print(str_glue("[{dom}] [{mod}] Mosaicking"))
  tic(str_glue("   Done mosaicking")) # 7
  
  if(mod == "Had") last_yr <- 2099 else last_yr <- 2100
  
  # loop across variables
  c("fd", "su", "id", "tr",
    "gsl",
    "txx", "tnx", "txn", "tnn",
    "tn10p", "tx10p", "tn90p", "tx90p",
    "wsdi", "csdi", "dtr",
    "rx1day", "rx5day",
    "sdii", "r10mm", "r20mm", "rnnmm",
    "cdd", "cwd",
    "r95ptot", "r99ptot", "prcptot",
    str_glue("biovar{seq(1,19) %>% str_pad(2, 'left', '0')}")) %>% 
    
    walk(function(v){
      
      print(str_glue("Processing variable {v}"))
      
      
      print(str_glue("   Reading chunks"))
      
      list.files(dir_tmp_chunks, full.names = T) %>% 
        .[str_detect(., v)] %>% 
        map(readRDS) -> chunks
      
      print(str_glue("   Mosaicking chunks"))
      
      map(chunks, function(s){
        
        st_set_dimensions(s, "lon", 
                          values = st_get_dimension_values(s, "lon") %>% round(1)) -> s
        
        st_set_dimensions(s, "lat", 
                          values = st_get_dimension_values(s, "lat") %>% round(1)) -> s
        
        s %>% st_set_crs(4326) -> s
        
        s %>% 
          as("Raster")
        
      }) %>% 
        do.call(merge, .) %>% 
        st_as_stars() %>% 
        st_set_dimensions("band", 
                          values = dates %>% .[month(.) == 1 & day(.) == 1], 
                          name = "time") %>% 
        setNames(v) -> var_mosaic
      
      print(str_glue("   Saving mosaic"))
      
      func_write_nc(var_mosaic,
                    str_glue("~/bucket_mine/results/global_climchgeindices_ww/{v}_{mod}_{dom}_annual_1970_{last_yr}.nc"))
      
    }) # end of loop across variables
  
  toc() # 7
  
  unlink(dir_tmp_chunks, recursive = T)
  unlink(dir_down, recursive = T)
  
  print(str_glue(" "))
  toc() # 1
  
} # end of model loop

