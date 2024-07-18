This project is the beginning process of a financial data application that uses commodity futures tick data. The data is read into 
H5PY tables and stored in the .h5 extension on your machine. The data is currently stored in an AWS S3 bucket. The current version of the 
program connects to AWS, reads and converts the tick data into numpy arrays, and stores the arrays in .h5 files. The rest of
the project involves code that allows the user to create user defined time bars, volume weighted average price bars, renko bars, and other
types of data buckets that do not involve a time component. There is also code to perform regression analysis between two commodities or between
different delvery months of the same commodity future. The data set includes 5 commodity grain futures traded at the Chicago Board of Trade
division of the CME. They are corn, wheat, soybeans, soybean meal, and soybean oil.

There are a few requirements in order to view this project. They are the following:

1.The project uses python 3.12.3
2. You will need to install Conda on your machine.  https://www.anaconda.com/download
3. You will need to install HDF5. This project uses version 1.14.4. https://www.hdfgroup.org/downloads/hdf5/
4. To view your .h5 files, you will need HDF5 Viewer installed. https://www.hdfgroup.org/downloads/hdfview/
5. Follow the requirements.txt in the repository to download the rest of the needed libraries.
6. Once everything is installed, simply run this command from your command line, python process_ticks_from_s3.py.
7. Use your HDF5 Viewer application to view the data as it is currently stored in your .h5 files.
8. There is an evironment.yml file created by and for conda. This may be a better alternative than requirements.txt.

