setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../scripts/h2o-r-test-setup.R")
################################################################################
##
## This tests Orc multifile parser by comparing the summary of the original csv frame with the h2o parsed orc frame
##
################################################################################


test.continuous.or.categorical <- function() {


	original = h2o.importFile(locate("bigdata/laptop/parser/orc/Datafor_pubdev3200/airlines_all.05p.csv"),destination_frame = "original",
                     col.types=c("Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum"))
	csv = h2o.importFile(locate("bigdata/laptop/parser/orc/Datafor_pubdev3200/air05_csv"),destination_frame = "csv",col.names = names(original),
                     col.types=c("Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum"))
	orc = h2o.importFile(locate("bigdata/laptop/parser/orc/Datafor_pubdev3200/air05_orc"),destination_frame = "orc",col.names = names(original),
                     col.types=c("Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum"))

  	expect_equal(summary(csv),summary(original))
  	
  	for(i in 1:ncol(csv)){
       print(i)
       expect_equal(summary(csv[,i]),summary(orc[,i]))
    }
}

doTest("Test orc multifile parser", test.continuous.or.categorical)
