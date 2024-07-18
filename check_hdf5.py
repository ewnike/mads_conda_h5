"""
code to test if
h5py installed and
to verify HDF5 installed
and the version.
"""

import h5py  # pylint: disable=import-error

print(h5py.version.info)
