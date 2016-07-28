from __future__ import print_function
import sys
sys.path.insert(1,"../../")
import h2o
from tests import pyunit_utils


def import_folder():
    """
    This test will build a H2O frame from importing the bigdata/laptop/parser/orc/milsongs_orc_csv
    from and build another H2O frame from the multi-file orc parser using multiple orc files that are
    saved in the directory bigdata/laptop/parser/orc/milsongs_orc.  It will compare the two frames
    to make sure they are equal.
    :return: None if passed.  Otherwise, an exception will be thrown.
    """

    tol_time = 200              # comparing in ms or ns
    tol_numeric = 1e-5          # tolerance for comparing other numeric fields
    numElements2Compare = 10   # choose number of elements per column to compare.  Save test time.

    multi_file_csv = h2o.import_file(path=pyunit_utils.locate("bigdata/laptop/parser/orc/milsongs_orc_csv"))
    multi_file_orc = h2o.import_file(path=pyunit_utils.locate("bigdata/laptop/parser/orc/milsongs_orc"))

    # make sure orc multi-file and single big file create same H2O frame
    assert pyunit_utils.compare_frames(multi_file_orc , multi_file_csv, numElements2Compare, tol_time, tol_numeric), \
        "H2O frame parsed from multiple orc and single csv files are different!"


if __name__ == "__main__":
    pyunit_utils.standalone_test(import_folder)
else:
    import_folder()
