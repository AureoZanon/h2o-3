setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../scripts/h2o-r-test-setup.R")
################################################################################
##
## Verify that we can specify data column type in R as well
##
################################################################################


test.continuous.or.categorical <- function() {

  airline_part0_csv <- h2o.importFile(locate("bigdata/laptop/parser/orc/airlines_import_type/000000_0.csv"))
  airline_part0_orc <- h2o.importFile(locate("bigdata/laptop/parser/orc/airlines_import_type/000000_0.orc"),col.names = names(airline_part0_csv),col.types = c("Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum"))

  h2o_and_h2o_equal(airline_part0_csv, airline_part0_orc)   # compare two frames and make sure they are equal

  # Nidhi:  Please add frame summary comparison.  In particular, we would like to compare the sizes of the
  # two frames.  They should be close within some tolerance.  In flow, they are both about 4MB.

}

doTest("Veryfying R Orc Parser Can Declare Types on Import", test.continuous.or.categorical)

