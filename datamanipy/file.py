import os
import pyreadstat


class File():

    """
    A class used to represent a file.

    Arguments
    ---------
    path : str
        Path of the file
    encoding : str, optional, default is platform dependent
        Encoding used to read the file

    Attributes
    ----------
    path : str
        Path of the sas7bdat file
    encoding : str
        Encoding used to read the file
    directory : str
        Directory in which the file is stored
    file : str
        Name of the file with its extension
    name : str
        Name of the file without its extension
    extension : str
        Extension of the file

    Methods
    -------
    remove :
        Delete the file
    open : 
        Open file and return a corresponding file object
    create :
        Create the file if it does not already exist
    """

    def __init__(self, path, encoding=None):
        self.path = path
        self.encoding = encoding
        self.directory, self.file = os.path.split(self.path)
        self.name, self.extension = os.path.splitext(self.file)
        with self.open() as f:
            pass

    def remove(self):
        """Delete the file"""
        os.remove(self.path)

    def open(self, mode='r', buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        """Open file and return a corresponding file object
        See https://docs.python.org/3/library/functions.html#open for the full documentation.
        """
        return open(file=self.path, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline, closefd=closefd, opener=opener)

    def create(self):
        """Create the file if it does not already exist"""
        with self.open(mode='x') as f:
            print(f'File {self.file} created in {self.directory}')
            pass


class Sas(File):

    """
    A class used to represent a sas7bdat file.
    This class is a child of the File class and therefore inherits the same attributes and methods.

    Methods
    -------
    read :
        Read a sas7bdat file
    to_csv :
        Convert sas7bdat file into csv file
    """

    def __init__(self, path, encoding='latin1'):
        super(Sas, self).__init__(path, encoding)

        if self.extension != '.sas7bdat':
            raise TypeError(f'{self.file} is not a sas7bdat file')

    def read(self, row_limit=0):
        """Read a sas7bdat file

        Parameters
        ----------
        row_limit : int, optional, default is 0 meaning unlimited
            Maximum number of rows to read
        
        Returns
        -------
        pandas.DataFrame with the data
        """
        return pyreadstat.read_sas7bdat(
            self.path,
            dates_as_pandas_datetime=True,
            encoding=self.encoding,
            row_limit=row_limit)

    def read_in_chunks(self, row_limit=0, chunksize=10000):
        """Read a sas7bdat file

        Parameters
        ----------
        row_limit : int, optional, default is 0 meaning unlimited
            Maximum number of rows to read
        chunksize : int, optional, default is 10000
            Size of the chunks to read
        
        Yields
        -------
        - pandas.DataFrame with the data
        - a generator that reads the file in chunks.
        """
        return pyreadstat.read_file_in_chunks(
            read_function=pyreadstat.read_sas7bdat,
            file_path=self.path,
            limit=row_limit,
            chunksize=chunksize,
            encoding=self.encoding)

    def to_csv(self):
        """Convert SAS file into CSV file"""

        csv_file = self.name + ".csv"
        csv_path = os.path.join(self.directory, self.name + '.csv')

        try:
            # if another csv file exists under the same name, remove it
            os.remove(csv_path)
            print(
                f'Warning : csv file {csv_file} already existed and has been removed.')
        except:
            pass

        # As the size of .sas7bdat files is often large, we read it by chunks
        reader = self.read_in_chunks()
        header = True
        for df, _ in reader:
            df.to_csv(csv_path, header=header, encoding=self.encoding,
                      mode='a', sep=';', index=False)
            header = False
        
        print(f'Sas file {self.file} converted to csv file {csv_file}')
        return None

