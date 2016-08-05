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


import logging
import os
from datetime import datetime
import json
import numpy as np
import pandas as pd
from HydraLib.HydraException import HydraPluginError

from HydraLib import config, hydra_dateutil
from csv_util import validate_value
import pytz
import re

global seasonal_key
seasonal_key = None

global time_formats
time_formats = {}

log = logging.getLogger(__name__)

def create_dataset(value,
                   resource_attr,
                   unit,
                   dimension,
                   resource_name,
                   metadata,
                   restriction_dict,
                   expand_filenames,
                   basepath,
                   file_dict,
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


    dataset          = dict(
        id=None,
        type=None,
        unit=None,
        dimension=None,
        name=dataset_name,
        value=None,
        hidden='N',
        metadata=None,
    )

    resourcescenario['attr_id'] = resource_attr['attr_id']
    resourcescenario['resource_attr_id'] = resource_attr['id']

    value = value
    if unit is not None:
        unit = unit
        if len(unit) == 0:
            unit = None
    data_columns = None
    try:
        float(value)
        dataset['type'] = 'scalar'
        scal = create_scalar(value, restriction_dict)
        dataset['value'] = scal
    except ValueError:
        #Check if it's an array or timeseries by first seeing if the value points
        #to a valid file.
        value = value.replace('\\', '/')
        try:
            filedata = []
            if expand_filenames:
                full_file_path = os.path.join(basepath, value)
                if file_dict.get(full_file_path) is None:
                    with open(full_file_path) as f:
                        filedata = []
                        for l in f:
                            l = re.sub('\s*,\s*', ',', l)
                            l = re.sub('^ *', '', l)
                            l = re.sub(' *$', '', l)
                            l = l.replace('\n', '').replace('\r', '').split(',')
                            filedata.append(l)
                        file_dict[full_file_path] = filedata
                else:
                    filedata = file_dict[full_file_path]



                #The name of the resource is how to identify the data for it.
                #Once this the correct line(s) has been identified, remove the
                #name from the start of the line
                data = []
                for l in filedata:

                    l_resource_name = l[0]
                    if l_resource_name == resource_name:
                        data.append(l[1:])

                if len(data) == 0:
                    log.info('%s: No data found in file %s' %
                                 (resource_name, value))
                    raise HydraPluginError('%s: No data found in file %s' %
                                         (resource_name, value))
                else:
                    if is_timeseries(data):
                        data_columns = get_data_columns(filedata)
                         
                        ts = create_timeseries( data,
                                                restriction_dict=restriction_dict,
                                                data_columns=data_columns,
                                                filename=value,
                                                timezone=timezone)
                       
                        dataset['type'] = 'timeseries' 
                        dataset['value'] = ts
                    else:
                        dataset['type'] = 'array'
                        if len(filedata) > 0:
                            try:
                                dataset['value'] = create_array(data[0], restriction_dict)
                            except Exception, e:
                                log.exception(e)
                                raise HydraPluginError("There is a value "
                                                       "error in %s. "
                                                       "Please check value"
                                                       " %s is correct."%(value, data[0]))
                        else:
                            dataset['value'] = None
            else:
                raise IOError
        except IOError, e:
            dataset['type'] = 'descriptor'
            desc = create_descriptor(value, restriction_dict)
            dataset['value'] = desc
    
    if unit is not None:
        dataset['unit'] = unit
    if dimension is not None:
        dataset['dimension'] = dimension

    dataset['name'] = default_name

    resourcescenario['value'] = dataset

    m = {}

    if metadata:
        m = metadata

    if data_columns:
        m['data_struct'] = '|'.join(data_columns)

    m = json.dumps(m)

    dataset['metadata'] = m

    return resourcescenario

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

def create_timeseries(data, restriction_dict={}, data_columns=None, filename="", timezone=pytz.utc):
    if len(data) == 0:
        return None
    
    if data_columns is not None:
        col_headings = data_columns
    else:
        col_headings =[str(idx) for idx in range(len(data[0][2:]))]
    
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
                                   "in %s where the value = %s)"%( ts_time,
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
