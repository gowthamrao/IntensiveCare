library(magrittr)
output2 <- output %>% 
  dplyr::filter(sourceKey == 'OPTUM_EXTENDED_DOD' & sqlFileName == 'intensiveCareRevenueCodeMedical.sql' & domain %in% c('Procedure', 'Observation'))


readr::write_csv(x = output2, path = paste0(path, '/results/output.csv'))