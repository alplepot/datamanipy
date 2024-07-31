import os
import pyreadstat
import pandas


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


class Csv(File):

    """
    A class used to represent a csv file.
    This class is a child of the File class and therefore inherits the same attributes and methods.

    Methods
    -------
    to_df :
        Import a csv file into a pandas.DataFrame
    to_df_in_chunks :
        Import a csv file into a pandas.DataFrame in chunks
    """

    def __init__(self, path, encoding='latin1'):
        super(Csv, self).__init__(path, encoding)

        if self.extension != '.csv':
            raise TypeError(f'{self.file} is not a csv file')

    def to_df(self,
              sep=';',
              delimiter=None,
              header='infer',
              names=None,
              index_col=None,
              usecols=None,
              squeeze=None,
              mangle_dupe_cols=True,
              dtype=None, engine=None,
              converters=None,
              true_values=None,
              false_values=None,
              skipinitialspace=False,
              skiprows=None,
              skipfooter=0,
              nrows=None,
              na_values=None,
              keep_default_na=True,
              na_filter=True,
              verbose=False,
              skip_blank_lines=True,
              parse_dates=None,
              infer_datetime_format=False,
              keep_date_col=False,
              date_parser=None,
              dayfirst=False,
              cache_dates=True,
              iterator=False,
              chunksize=None,
              compression='infer',
              thousands=None,
              decimal='.',
              lineterminator=None,
              quotechar='"',
              quoting=0,
              doublequote=True,
              escapechar=None,
              comment=None,
              encoding_errors='strict',
              dialect=None,
              error_bad_lines=None,
              warn_bad_lines=None,
              on_bad_lines=None,
              delim_whitespace=False,
              low_memory=True,
              memory_map=False,
              float_precision=None,
              storage_options=None):
        """Import csv into a pandas.DataFrame
        See https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html for the full documentation.
        """

        return pandas.read_csv(self.path, sep=sep, delimiter=delimiter, header=header, names=names,
                               index_col=index_col, usecols=usecols, squeeze=squeeze,
                               mangle_dupe_cols=mangle_dupe_cols, dtype=dtype, engine=None, converters=converters,
                               true_values=true_values, false_values=false_values,
                               skipinitialspace=skipinitialspace, skiprows=skiprows, skipfooter=skipfooter,
                               nrows=nrows, na_values=na_values, keep_default_na=keep_default_na,
                               na_filter=na_filter, verbose=verbose, skip_blank_lines=skip_blank_lines,
                               parse_dates=parse_dates, infer_datetime_format=infer_datetime_format,
                               keep_date_col=keep_date_col, date_parser=date_parser,
                               dayfirst=dayfirst, cache_dates=cache_dates,
                               iterator=iterator, chunksize=chunksize, compression=compression,
                               thousands=thousands, decimal=decimal, lineterminator=lineterminator,
                               quotechar=quotechar, quoting=quoting, doublequote=doublequote,
                               escapechar=escapechar, comment=comment, encoding=self.encoding,
                               encoding_errors=encoding_errors, dialect=dialect, error_bad_lines=error_bad_lines,
                               warn_bad_lines=warn_bad_lines, on_bad_lines=on_bad_lines,
                               delim_whitespace=delim_whitespace, low_memory=low_memory, memory_map=memory_map,
                               float_precision=float_precision, storage_options=storage_options)


class Sas(File):

    """
    A class used to represent a sas7bdat file.
    This class is a child of the File class and therefore inherits the same attributes and methods.

    Methods
    -------
    to_df :
        Import a sas7bdat file into a pandas.DataFrame
    to_df_in_chunks :
        Import a sas7bdat file into a pandas.DataFrame in chunks
    to_csv :
        Convert sas7bdat file into csv file
    """

    def __init__(self, path, encoding='latin1'):
        super(Sas, self).__init__(path, encoding)

        if self.extension != '.sas7bdat':
            raise TypeError(f'{self.file} is not a sas7bdat file')

    def to_df(self, nrows=None):
        """Import a sas7bdat file into a pandas.DataFrame

        Parameters
        ----------
        row_limit : int, optional, default is 0 meaning unlimited
            Maximum number of rows to read
        
        Returns
        -------
        pandas.DataFrame with the data
        """
        if nrows is None:
            nrows = 0
        return pyreadstat.read_sas7bdat(
            self.path,
            dates_as_pandas_datetime=True,
            encoding=self.encoding,
            row_limit=nrows)

    def to_df_in_chunks(self, nrows=None, chunksize=10000):
        """Import a sas7bdat file into a pandas.DataFrame by chunks

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
        if nrows is None:
            nrows = 0
        return pyreadstat.read_file_in_chunks(
            read_function=pyreadstat.read_sas7bdat,
            file_path=self.path,
            limit=nrows,
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


class Excel(File):
    
    """
    A class used to represent a Excel file.
    This class is a child of the File class and therefore inherits the same attributes and methods.

    Methods
    -------
    to_df :
        Import a sas7bdat file into a pandas.DataFrame
    """

    def __init__(self, path, encoding='latin1'):
        super(Excel, self).__init__(path, encoding)

        if self.extension != '.xls' and self.extension != '.xlsx':
            raise TypeError(f'{self.file} is not a excel file')
    
    def to_df(self,
              sheet_name=0,
              header=0,
              names=None,
              index_col=None,
              usecols=None,
              squeeze=None,
              dtype=None, 
              engine=None, 
              converters=None, 
              true_values=None, 
              false_values=None, 
              skiprows=None, 
              nrows=None, 
              na_values=None, 
              keep_default_na=True, 
              na_filter=True, 
              verbose=False, 
              parse_dates=False, 
              date_parser=None, 
              thousands=None,
              comment=None, 
              skipfooter=0, 
              convert_float=None, 
              mangle_dupe_cols=True, 
              storage_options=None):
        """Import csv into a pandas.DataFrame
        See https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html for the full documentation.
        """
    
        return pandas.read_excel(self.path, sheet_name=sheet_name, header=header, names=names, index_col=index_col, usecols=usecols, 
                                  squeeze=squeeze, dtype=dtype, engine=engine, converters=converters, true_values=true_values, 
                                  false_values=false_values, skiprows=skiprows, nrows=nrows, na_values=na_values, keep_default_na=keep_default_na, 
                                  na_filter=na_filter, verbose=verbose, parse_dates=parse_dates, date_parser=date_parser, 
                                  thousands=thousands, comment=comment, skipfooter=skipfooter, convert_float=convert_float, 
                                  mangle_dupe_cols=mangle_dupe_cols, storage_options=storage_options)