from __future__ import print_function
import sys
sys.path.insert(1,"../../")
import h2o
from tests import pyunit_utils
#----------------------------------------------------------------------
# Purpose:  This test will test orc-parser in HDFS.
#----------------------------------------------------------------------


def hdfs_orc_parser():

    # Check if we are running inside the H2O network by seeing if we can touch
    # the namenode.
    hadoop_namenode_is_accessible = pyunit_utils.hadoop_namenode_is_accessible()

    if hadoop_namenode_is_accessible:
        numElements2Compare = 10
        tol_time = 200
        tol_numeric = 1e-5

        hdfs_name_node = pyunit_utils.hadoop_namenode()
        hdfs_orc_file = "/datasets/orc_parser/prostate_NB.orc"
        hdfs_csv_file = "/datesets/orc_parser/prostate_NB.csv"

        print("Importing prostate_NB.orc from HDFS")
        url_orc = "hdfs://{0}{1}".format(hdfs_name_node, hdfs_orc_file)
        orc_h2o = h2o.import_file(url_orc)

        print("Importing prostate_NB.csv from HDFS")
        url_csv = "hdfs://{0}{1}".format(hdfs_name_node, hdfs_csv_file)
        csv_h2o = h2o.import_file(url_csv)

        # compare the two frames and make sure they are the same
        assert pyunit_utils.compare_frames(orc_h2o, csv_h2o, numElements2Compare, tol_time, tol_numeric), \
            "H2O frame parsed from orc and csv files are different!"
    else:
        raise EnvironmentError


if __name__ == "__main__":
    pyunit_utils.standalone_test(hdfs_orc_parser)
else:
    hdfs_orc_parser()
