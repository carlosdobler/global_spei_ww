# Computation of the Standardized Precipitation-Evapotranspiration Index (SPEI).

spei <- function(data, scale, kernel=list(type='rectangular',shift=0),
                 distribution='log-Logistic', fit='ub-pwm', na.rm=FALSE, 
                 ref.start=NULL, ref.end=NULL, x=FALSE, params=NULL, ...) {
  
  scale <- as.numeric(scale)
  na.rm <- as.logical(na.rm)
  x <- as.logical(x)
  #if (!exists("data",inherits=F) | !exists("scale",inherits=F)) {
  #	stop('Both data and scale must be provided')
  #}
  if (sum(is.na(data))>0 & na.rm==FALSE) {
    stop('Error: Data must not contain NAs')
  }
  if (distribution!='log-Logistic' & distribution!='Gamma' & distribution!='PearsonIII') {
    stop('Distrib must be one of "log-Logistic", "Gamma" or "PearsonIII"')
  }
  if (fit!='max-lik' & fit!='ub-pwm' & fit!='pp-pwm') {
    stop('Method must be one of "ub-pwm" (default), "pp-pwm" or "max-lik"')
  }
  if ({!is.null(ref.start) & length(ref.start)!=2} | {!is.null(ref.end) & length(ref.end)!=2}) {
    stop('Start and end of the reference period must be a numeric vector of length two.')
  }
  
  if (!is.ts(data)) {
    data <- ts(as.matrix(data), frequency = 12)
  } else {
    data <- ts(as.matrix(data), frequency=frequency(data), start=start(data))
  }
  m <- ncol(data)
  fr <- frequency(data)
  
  if (distribution=='Gamma') {
    coef <- array(NA,c(2,m,fr),list(par=c('alpha','beta'),colnames(data),NULL))
    if (!is.null(params)) {
      if (dim(params)[1]!=2 | dim(params)[2]!=m | dim(params)[3]!=12) {
        stop(paste('parameters array should have dimensions (2,',m,'12)',sep=' '))
      }
    }
  }
  if (distribution=='log-Logistic') {
    coef <- array(NA,c(3,m,fr),list(par=c('xi','alpha','kappa'),colnames(data),NULL))
    if (!is.null(params)) {
      if (dim(params)[1]!=3 | dim(params)[2]!=m | dim(params)[3]!=12) {
        stop(paste('parameters array should have dimensions (3,',m,'12)',sep=' '))
      }
    }
  }
  if (distribution=='PearsonIII') {
    coef <- array(NA,c(3,m,fr),list(par=c('mu','sigma','gamma'),colnames(data),NULL))
    if (!is.null(params)) {
      if (dim(params)[1]!=3 | dim(params)[2]!=m | dim(params)[3]!=12) {
        stop(paste('parameters array should have dimensions (3,',m,'12)',sep=' '))
      }
    }
  }
  
  # Loop through series (columns in data)
  if (!is.null(ref.start) & !is.null(ref.end)) {
    data.fit <- window(data,ref.start,ref.end)	
  } else {
    data.fit <- data
  }
  std <- data*NA
  for (s in 1:m) {
    # Cumulative series (acu)
    acu <- data.fit[,s]
    acu.pred <- data[,s]
    if (scale>1) {
      wgt <- kern(scale,kernel$type,kernel$shift)
      acu[scale:length(acu)] <- rowSums(embed(acu,scale)*wgt,na.rm=na.rm)
      acu[1:{scale-1}] <- NA
      acu.pred[scale:length(acu.pred)] <- rowSums(embed(acu.pred,scale)*wgt,na.rm=na.rm)
      acu.pred[1:{scale-1}] <- NA
    }
    
    # Loop through the months
    for (c in (1:fr)) {
      # Filter month m, excluding NAs
      f <- which(cycle(acu)==c)
      f <- f[!is.na(acu[f])]
      ff <- which(cycle(acu.pred)==c)
      ff <- ff[!is.na(acu.pred[ff])]
      
      # Monthly series, sorted
      month <- sort(acu[f])
      
      if (length(month)==0) {
        std[f] <- NA
        next()
      }
      
      if (is.null(params)) {
        if (is.na(sd(month,na.rm=TRUE)) | (sd(month, na.rm=TRUE) == 0)) {
          std[f] <- NA
          next()
        }
        if (distribution=='log-Logistic') {
          # Fit a generalized log-Logistic distribution
          if (fit=='pp-pwm') {
            pwm <- pwm.pp(month,-0.35,0)
          } else {
            pwm <- pwm.ub(month)
          }
          lmom <- pwm2lmom(pwm)
          if (!are.lmom.valid(lmom) | is.na(sum(lmom[[1]])) | is.nan(sum(lmom[[1]]))) {
            next()
          }
          llpar <- parglo(lmom)
          if (fit=='max-lik') {
            llpar <- parglo.maxlik(month,llpar$para)
          }
          # Compute standardized values
          std[ff,s] <- qnorm(pglo(acu.pred[ff],llpar))
          coef[,s,c] <- llpar$para
        } else if (distribution=='Gamma' | distribution=='PearsonIII') {
          # Probability of monthly precipitation = 0 (pze)
          zeros <- sum(month==0)
          pze <- sum(month==0)/length(month)
          if (fit=='pp-pwm') {
            pwm <- pwm.pp(month[month>0],-0.35,0)
          } else {
            pwm <- pwm.ub(month[month>0])
          }
          lmom <- pwm2lmom(pwm)
          if (!are.lmom.valid(lmom) | is.na(sum(lmom[[1]])) | is.nan(sum(lmom[[1]]))) {
            next()
          }					
          if (distribution =='Gamma') {
            # Fit a Gamma distribution
            gampar <- pargam(lmom)
            # Compute standardized values
            std[ff,s] <- qnorm(cdfgam(acu.pred[ff],gampar))
            std[ff,s] <- qnorm(pze + (1-pze)*pnorm(std[ff,s]))
            coef[,s,c] <- gampar$para
          } else if (distribution =='PearsonIII') {
            # Fit a PearsonIII distribution
            p3par <- parpe3(lmom)
            # Compute standardized values
            std[ff,s] <- qnorm(cdfpe3(acu.pred[ff],p3par))
            std[ff,s] <- qnorm(pze + (1-pze)*pnorm(std[ff,s]))
            coef[,s,c] <- p3par$para
          } # end if
        } # end if
      } else {
        if (dim(params)[1]!=3 & dim(params)[2]!=m & dim(params)[3]!=12) {
          stop(paste('params should be an array with dimensions (3,',m,',12)',sep=' '))
        }
        coef[,s,c] <- params[,s,c]
        if (distribution=='log-Logistic') {
          std[ff,s] <- qnorm(pglo(acu.pred[ff],
                                  list(type="glo", para=params[,s,c], source="user")))
        } else {
          if (distribution =='Gamma') {
            std[ff,s] <- qnorm(cdfgam(acu.pred[ff],
                                      list(type="gam", para=params[,s,c], source="user")))
            std[ff,s] <- qnorm(pze + (1-pze)*pnorm(std[ff,s]))
          } else if (distribution =='PearsonIII') {
            std[ff,s] <- qnorm(cdfpe3(acu.pred[ff],
                                      list(type="pe3", para=params[,s,c], source="user")))
            std[ff,s] <- qnorm(pze + (1-pze)*pnorm(std[ff,s]))
          }					
        }
      }
    } # next c (month)
  } # next s (series)
  colnames(std) <- colnames(data)
  
  z <- list(call=match.call(expand.dots=FALSE),
            fitted=std,coefficients=coef,scale=scale,kernel=list(type=kernel$type,
                                                                 shift=kernel$shift,values=kern(scale,kernel$type,kernel$shift)),
            distribution=distribution,fit=fit,na.action=na.rm)
  if (x) z$data <- data
  if (!is.null(ref.start)) z$ref.period <- rbind(ref.start,ref.end)
  
  class(z) <- 'spei'
  return(z)
}

