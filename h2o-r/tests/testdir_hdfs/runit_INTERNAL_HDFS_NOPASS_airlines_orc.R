setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../scripts/h2o-r-test-setup.R")
#----------------------------------------------------------------------
# Purpose:  This tests GLRM on a large dataset.
#----------------------------------------------------------------------




#----------------------------------------------------------------------
# Parameters for the test.
#----------------------------------------------------------------------

# Check if we are running inside the H2O network by seeing if we can touch
# the namenode.
hadoop_namenode_is_accessible = hadoop.namenode.is.accessible()

if (hadoop_namenode_is_accessible) {
  hdfs_name_node = HADOOP.NAMENODE
  hdfs_air_orc = "/datasets/airlines_all_orc_parts"
  hdfs_air_original = "/datasets/airlines_all.csv"
} else {
  stop("Not running on H2O internal network. No access to HDFS.")
}

#----------------------------------------------------------------------

heading("BEGIN TEST")
check.hdfs_airorc <- function() {

  
  
  heading("Import airlines 116M dataset in original csv format ")
  url <- sprintf("hdfs://%s%s", hdfs_name_node, hdfs_air_original)
  csv.hex <- h2o.importFile(url)
  n <- nrow(csv.hex)
  print(paste("Imported n =", n, "rows from csv"))
  
  heading("Import airlines 116M dataset in ORC format ")
  url <- sprintf("hdfs://%s%s", hdfs_name_node, hdfs_air_orc)
  orc.hex <- h2o.importFolder(url,destination_frame = "dd",col.names = names(csv.hex),
                      col.types = c("Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Numeric",
                      "Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum","Numeric","Numeric","Numeric","Numeric"
                      ,"Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Enum","Enum"))
  n <- nrow(orc.hex)
  print(paste("Imported n =", n, "rows from orc"))


  expect_equal(dim(orc.hex),dim(csv.hex))
  expect_equal(summary(orc.hex),summary(csv.hex))
  
  h2o.rm(orc.hex)   # remove file


  
}

doTest("ORC multifile parse test", check.hdfs_airorc)