# (c) Copyright 2013, 2014, 2015 University of Manchester
#
# ImportCSV is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ImportCSV is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with ImportCSV.  If not, see <http://www.gnu.org/licenses/>
#

import os
import logging
import json
import re
from datetime import datetime

import pytz
import numpy as np
import pandas as pd

from hydra_base.exceptions import HydraPluginError
from hydra_base.util import config, hydra_dateutil

from .csv_util import validate_value

import hydra_pywr_common


global seasonal_key
seasonal_key = None

global time_formats
time_formats = {}

log = logging.getLogger(__name__)

def create_dataset(value,
                   resource_attr,
                   unit_id,
                   resource_name,
                   metadata,
                   restriction_dict,
                   expand_filenames,
                   basepath,
                   file_dict, #stores files in memory to avoid re-reading them
                   default_name,
                   timezone,
                  ):

    resourcescenario = dict()

    global seasonal_key
    if seasonal_key is None:
        seasonal_key = config.get('DEFAULT', 'seasonal_key', '9999')

    if metadata.get('name'):
        dataset_name = metadata['name']
        del(metadata['name'])
    else:
        dataset_name = 'Import CSV data'

    dataset = dict(
        id=None,
        type=None,
        unit_id=None,
        name=dataset_name,
        value=None,
        hidden='N',
        metadata=None,
    )

    resourcescenario['attr_id'] = resource_attr['attr_id']
    resourcescenario['resource_attr_id'] = resource_attr['id']

    data_columns = None
    try:
        float(value)
        dataset['type'] = 'scalar'
        scal = create_scalar(value, restriction_dict)
        dataset['value'] = scal
    except ValueError:
        #Check if it's an array or timeseries by first seeing if the value points
        #to a valid file.
        if expand_filenames:
            value = value.replace('\\', os.sep)
            full_file_path = os.path.join(basepath, value)
            if os.path.exists(full_file_path):
                data_type, file_value = get_data_from_file(full_file_path,
                                                           resource_name,
                                                           restriction_dict,
                                                           timezone,
                                                           file_dict)
                dataset['type'] = data_type
                dataset['value'] = file_value

    if dataset['value'] is None:
        #still null, so default to descriptor
        dataset['type'] = 'descriptor'
        desc = create_descriptor(value, restriction_dict)
        dataset['value'] = desc

    if unit_id is not None:
        dataset['unit_id'] = unit_id

    dataset['name'] = default_name

    resourcescenario['dataset'] = dataset

    if data_columns:
        metadata['data_struct'] = '|'.join(data_columns)

    dataset['metadata'] = json.dumps(metadata)

    return resourcescenario

def _get_nd_array(numpyarray, shape):
    #Numpy arrays have int64 values, which are not JSON compatible,
    #so we iterate through the array to convert any int64 values to floats
    array_with_floats = []
    if isinstance(numpyarray[0], np.ndarray) and len(numpyarray) == 1:
        numpyarray = numpyarray[0]

    for a in numpyarray:
        if isinstance(a, np.int64):
            array_with_floats.append(float(a))
        else:
            array_with_floats.append(a)
    np_array_with_floats = np.reshape(array_with_floats, shape)
    value = json.dumps(np_array_with_floats.tolist())

    return value

def _get_shape_as_list(df_shape):
    """
    get a shape in the form '1 2' or '1.0 2.0' and return [1, 2]
    """
    #Here we assume that ALL the values for a particular node have the
    #same shape, so we just pick the first one.
    shape_array = df_shape.values[0].split(" ")
    #now make the values integers
    shape_array = [int(float(i)) for i in shape_array]

    return shape_array

def get_data_from_file(filepath, resource_name, restriction_dict, timezone, file_dict):
    value = None
    data_type = None
    #First check if it's an unknown value (a json file)
    if is_unknown_value(filepath):
        data_type, value = read_json_data(filepath, resource_name=None, attr_name=None)
    else:
        #avoid constantly re-opening the same csv file
        if file_dict.get(filepath) is not None:
            df = file_dict['filepath']
            #remove whitespace from the index
        else:
            df = pd.read_csv(filepath, index_col=0, skipinitialspace=True, parse_dates=True, comment='#')
            file_dict['filepath'] = df
            df.index = [i.strip() if isinstance(i, str) else i for i in df.index ]

        #Get all the rows wwhere the index is the resource name
        resource_df = df.loc[resource_name]

        #If pandas makeds the value a series, convert it back into a dataframe
        if isinstance(resource_df, pd.Series):
            resource_df = resource_df.to_frame().T

        if 'index' in resource_df.columns:
            resource_df = resource_df.set_index(['index'])

        #shape is used to allow csv to store multi-dimensional arrays
        df_shape = resource_df.get('shape').astype(str)
        shape_array = None
        if df_shape is not None:
            shape_array = _get_shape_as_list(df_shape)
            del resource_df['shape']

        if 'index' not in resource_df.columns and resource_df.index.name != 'index':
            data_type = 'array'
            if df_shape is not None:
                value = _get_nd_array(resource_df.values, shape_array)

        #todo, figure out how to reshape the data here...
        elif isinstance(resource_df.index, pd.DatetimeIndex) or 'XXXX' in resource_df.index[0]:
            if 'XXXX' in resource_df.index[0]:
                resource_df.index = [i.replace('XXXX', '9999') for i in resource_df.index]
            data_type = 'timeseries'
        else:
            data_type = 'dataframe'
            #try to turn the index into an int-based index instead of decimal if possible
            try:
                resource_df.index = resource_df.astype(int)
            except:
                pass

        if value is None:
            value = resource_df.to_json()


        validate_value(value, restriction_dict)

    return data_type, value


def create_scalar(value, restriction_dict={}):
    """
        Create a scalar (single numerical value) from CSV data
    """
    validate_value(value, restriction_dict)
    scalar = str(value)
    return scalar

def create_descriptor(value, restriction_dict={}):
    """
        Create a scalar (single textual value) from CSV data
    """
    validate_value(value, restriction_dict)
    descriptor = value
    return descriptor

def create_timeseries(data, restriction_dict={}, data_columns=None,
                      filename="", timezone=pytz.utc):
    if len(data) == 0:
        return None

    if data_columns is not None:
        col_headings = data_columns
    else:
        col_headings = [str(idx) for idx in range(len(data[0][2:]))]

    date = data[0][0]
    global time_formats
    timeformat = time_formats.get(date)
    if timeformat is None:
        timeformat = hydra_dateutil.guess_timefmt(date)
        time_formats[date] = timeformat

    seasonal = False

    if 'XXXX' in timeformat or seasonal_key in timeformat:
        seasonal = True

    ts_values = {}
    for col in col_headings:
        ts_values[col] = {}
    ts_times = [] # to check for duplicae timestamps in a timeseries.
    timedata = data
    for dataset in timedata:

        if len(dataset) == 0 or dataset[0] == '#':
            continue

        tstime = datetime.strptime(dataset[0], timeformat)
        tstime = timezone.localize(tstime)

        ts_time = hydra_dateutil.date_to_string(tstime, seasonal=seasonal)

        if ts_time in ts_times:
            raise HydraPluginError("A duplicate time %s has been found "
                                   "in %s where the value = %s)"%(ts_time,
                                                                  filename,
                                                                  dataset[2:]))
        else:
            ts_times.append(ts_time)

        value_length = len(dataset[2:])
        shape = dataset[1]
        if shape != '':
            array_shape = tuple([int(a) for a in
                                 shape.split(" ")])
        else:
            array_shape = (value_length,)

        ts_val_1d = []
        for i in range(value_length):
            ts_val_1d.append(str(dataset[i + 2]))

        try:
            ts_arr = np.array(ts_val_1d)
            ts_arr = np.reshape(ts_arr, array_shape)
        except:
            raise HydraPluginError("Error converting %s in file %s to an array"%(ts_val_1d, filename))

        ts_value = ts_arr.tolist()

        for i, ts_val in enumerate(ts_value):
            idx = col_headings[i]
            ts_values[idx][ts_time] = ts_val

    timeseries = json.dumps(ts_values)

    validate_value(pd.read_json(timeseries), restriction_dict)


    return timeseries

def create_array(dataset, restriction_dict={}):
    """
        Create a (multi-dimensional) array from csv data
    """
    #First column is always the array dimensions
    arr_shape = dataset[0]
    #The actual data is everything after column 0
    eval_dataset = []
    for d in dataset[1:]:
        try:
            d = eval(d)
        except:
            d = str(d)
        eval_dataset.append(d)
        #dataset = [eval(d) for d in dataset[1:]]

    #If the dimensions are not set, we assume the array is 1D
    if arr_shape != '':
        array_shape = tuple([int(a) for a in arr_shape.split(" ")])
    else:
        array_shape = (len(eval_dataset),)

    #Reshape the array back to its correct dimensions
    arr = np.array(eval_dataset)
    try:
        arr = np.reshape(arr, array_shape)
    except:
        raise HydraPluginError("You have an error with your array data."
                               " Please ensure that the dimension is correct."
                               " (array = %s, dimension = %s)" %(arr, array_shape))

    validate_value(arr.tolist(), restriction_dict)

    arr = json.dumps(arr.tolist())

    return arr

def is_timeseries(data):
    """
        Check whether a piece of data is a timeseries by trying to guess its
        date format. If that fails, it's not a time series.
    """
    try:
        date = data[0][0]

        global time_formats
        timeformat = time_formats.get(date)
        if timeformat is None:
            timeformat = hydra_dateutil.guess_timefmt(date)
            time_formats[date] = timeformat

        if timeformat is None:
            return False
        else:
            return True
    except:
        raise HydraPluginError("Unable to parse timeseries %s"%data)

def get_data_columns(filedata):
    """
        Look for column descriptors on the first line of the array and timeseries files
    """
    data_columns = None
    header = filedata[0]
    compressed_header = ','.join(header).replace(' ', '').lower()
    #Has a header been specified?
    if compressed_header.startswith('arraydescription') or \
        compressed_header.startswith('timeseriesdescription') or \
        compressed_header.startswith(','):

        #Get rid of the first column, which is the 'arraydescription' bit
        header_columns = header[1:]
        data_columns = []
        #Now get rid of the ',,' or ', ,', leaving just the columns.
        for h in header_columns:
            if h != "":
                data_columns.append(h)
    else:
        data_columns = None

    return data_columns

def is_unknown_value(filepath):
    """
        Check if this refers to a file which contains some 'unknown data'. That is
        to say, check it is a JSON file.
    """
    #TODO: Make this more robust but not by loading the json for EVERY file???
    return filepath.endswith('.json')

def read_json_data(filepath, resource_name=None, attr_name=None):
    """
        Read the data from a given json file. If a resource name and resource type
        are provided, it assumes data for multiple nodes are contained in the file
        and uses {resource_name}<>{resource_key} to access the data for the correct resource.
        If they are None (the default) then it assumes there is only data for 1 resource, and
        accesses the data at index [0]
        Args:
            filename: The FULL path to the data file
            resource_name: Node / Link / Group / Network Name
            attr_name: The name of the attribute
        Returns:
            A dataset with a data type as specified in the json, and the data value
            as speficied in the json
        Raises:
            HydraPluginError if the file or data cannot be found.
    """
    if not os.path.exists(filepath):
        raise HydraPluginError(f"Unable to read JSON file. File {filepath} does not exist.")

    filename = filepath.split(os.sep)[-1]

    json_data = {}
    with open(filepath, 'r') as json_file:
        json_data = json.load(json_file)

    if len(json_data) == 0:
        raise HydraPluginError(f"No data found in file {filename}")

    hydra_data_type = None
    value_json = None
    #By default just get the first value in the file
    data_to_process = list(json_data.values())[0]
    #if we have a resource and attribute name, look for that specific data
    if resource_name is not None and attr_name is not None:
        data_to_process = json_data.get(f'{resource_name}<>{attr_name}')
        if data_to_process is None:
            raise HydraPluginError(f"Unable to get the data for "
                                   f"{resource_name}<>{attr_name} in file {filename}")

    hydra_data_type = data_to_process.get('data_type')
    value_json = data_to_process.get('data')

    return hydra_data_type, json.dumps(value_json)
