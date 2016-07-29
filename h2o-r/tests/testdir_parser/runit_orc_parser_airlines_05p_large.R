setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../scripts/h2o-r-test-setup.R")
################################################################################
##
## Verify that we can specify data column type in R as well
##
################################################################################


test.continuous.or.categorical <- function() {

  airline_part0_csv <- h2o.importFile(locate("bigdata/laptop/parser/orc/airlines_import_type/000000_0.csv"))

  col.names = c("_col8", "_col16", "_col17", "_col22", "_col29", "_col30")
  col.types = c("enum", "enum", "enum", "enum", "enum", "enum")

  airline_part0_orc <- h2o.importFile(locate("bigdata/laptop/parser/orc/airlines_import_type/000000_0.orc"),
  col.types=list(by.col.name=col.names, types=col.types))

  h2o_and_h2o_equal(airline_part0_csv, airline_part0_orc)   # compare two frames and make sure they are equal

  # Nidhi:  Please add frame summary comparison.  In particular, we would like to compare the sizes of the
  # two frames.  They should be close within some tolerance.  In flow, they are both about 4MB.

}

doTest("Veryfying R Orc Parser Can Declare Types on Import", test.continuous.or.categorical)

