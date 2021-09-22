import unittest

"""
Tests in corecallbacks

First thing to do is to create a dummy dataframe which models the output of preprocessdata for df creation...

- preprocessdata
    Test that the dataframe validation is ok (i.e. a valid .txt file was passed to the program) 
    Test that duplicate files are overwritten 
    Test that dataframes are compared correctly (hardest thing to test...) 

"""

class TestCorecallbacks(unittest.TestCase):
    def test_preprocessdata(self):
        """
        Assuming a text file of appropriate format, and topo files of appropriate format, make sure that we can pre
        pre process a file into a dataframe. Also test what happens when the text file is corrupt.

        :return:
        """
        # TODO: Test case for corecallbacks.preprocessdata


if __name__ == "__main__":
    unittest.main()
