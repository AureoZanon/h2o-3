setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../scripts/h2o-r-test-setup.R")

# This test is written to make sure that warnings from Orc Parser are passed to the R client.
# In particulare, the first two Orc files contain unsupported column types.
# The third Orc file contains big integer values that are used by sentinel for H2O frame.

test.orc_parser.bad_data <- function() {
  # These files contain unsupported data types
  frame1 = h2o.importFile(locate("smalldata/parser/orc/TestOrcFile.testStringAndBinaryStatistics.orc"))
#  Sys.sleep(2)
#  expect_warning(h2o.importFile(locate("smalldata/parser/orc/TestOrcFile.testStringAndBinaryStatistics.orc")))

  frame2 = h2o.importFile(locate("smalldata/parser/orc/TestOrcFile.emptyFile.orc"))
#  Sys.sleep(2)
#  expect_warning(h2o.importFile(locate("smalldata/parser/orc/TestOrcFile.emptyFile.orc")))

  frame3 = h2o.importFile(locate("smalldata/parser/orc/nulls-at-end-snappy.orc"))
#  Sys.sleep(2)
#  # This file contains big integer value Long.MIN_VALUE that is used for sentinel
#  expect_warning(h2o.importFile(locate("smalldata/parser/orc/nulls-at-end-snappy.orc")))

}

doTest("Orc Parser: make sure warnings are passed to user.", test.orc_parser.bad_data)
