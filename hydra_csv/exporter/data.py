"""
    Extract the data values from datsets and return the appropriate
    string to place into the csv file
"""
import os
import json
import logging
from numpy import array

LOG = logging.getLogger(__name__)

def process_dataset(dataset, attribute_name, resource_name, resource_ref_key, target_dir):
    """
        Returns the value and metadata of a given dataset object
    """
    metadata = {}
    return_value = ''
    data_type = dataset.type.lower()

    if data_type == 'descriptor':
        return_value = _process_descriptor(dataset.value)
    elif data_type == 'array':
        return_value = _process_array(dataset.value,
                                      attribute_name,
                                      resource_name,
                                      resource_ref_key,
                                      target_dir)
    elif data_type == 'scalar':
        return_value = _process_scalar(dataset.value)
    elif data_type in ('dataframe', 'timeseries'):
        return_value = _process_dataframe(dataset.value,
                                          attribute_name,
                                          resource_name,
                                          resource_ref_key,
                                          target_dir,
                                          data_type = data_type)
    else:
        return_value = _process_unknown_value(dataset.value,
                                              attribute_name,
                                              resource_name,
                                              resource_ref_key,
                                              target_dir)

    if dataset is not None and dataset.metadata is not None:
        metadata = dataset.metadata

    return (str(return_value), metadata)

def _process_descriptor(value):
    return str(value)

def _process_scalar(value):
    return value

def _process_array(value, attr_name, resource_name, resource_ref_key, target_dir):
    file_name = "array_%s_%s.csv"%(resource_ref_key, attr_name)
    file_loc = os.path.join(target_dir, file_name)
    if os.path.exists(file_loc):
        arr_file = open(file_loc, 'a')
    else:
        arr_file = open(file_loc, 'w')

    arr_val = json.loads(value)

    flat_array, shape = flatten_array(arr_val)

    arr_file.write("%s,%s,%s\n" % (resource_name,
                                   ' '.join(shape),
                                   ','.join(flat_array)))

    arr_file.close()

    return file_name

def _process_dataframe(value, attr_name, resource_name, resource_ref_key,
                       target_dir, data_type='dataframe'):
    """
        Processes dataframes and timeseries.
    """
    if isinstance(value, str):
        value = json.loads(value)

    if value is None or value == {}:
        LOG.debug("Not exporting %s from resource %s as it is empty",
                  attr_name, resource_name)

    col_names = list(value.keys())
    file_name = "%s_%s_%s.csv"%(data_type, resource_ref_key, attr_name)
    file_loc = os.path.join(target_dir, file_name)
    if os.path.exists(file_loc):
        ts_file = open(file_loc, 'a')
    else:
        ts_file = open(file_loc, 'w')

        ts_file.write(",,,%s\n"%','.join(col_names))

    timestamps = value[col_names[0]].keys()
    ts_dict = {}
    for timestamp in timestamps:
        ts_dict[timestamp] = []

    for col, ts in value.items():
        for timestep, val in ts.items():
            ts_dict[timestep].append(val)

    for timestep, val in ts_dict.items():
        flat_array, shape = flatten_array(val)
        ts_file.write("%s,%s,%s,%s\n"%
                      (resource_name,
                       timestep,
                       ' '.join(shape),
                       ','.join(flat_array)))

    ts_file.close()

    return file_name

def _process_unknown_value(value, attr_name, resource_name, resource_ref_key, target_dir):
    try:

        #If it's json serialiseable, write it to the 'unknown data file' to preserve the data
        json_value = json.loads(value)
        filename = os.path.join(target_dir, 'unknown_values.json')
        data_key = f"{resource_name}_{resource_ref_key}_{attr_name}"
        if not os.path.exists(filename):
            with open(filename, 'w+', encoding='utf-8') as file:
                LOG.warning(filename)
                file_data = {data_key:json_value}
                json.dump(file_data, file, ensure_ascii=False, indent=4)
                file.close()
        else:

            with open(filename, 'r', encoding='utf-8') as file:
                tmp_data = json.load(file)
                tmp_data[data_key] = json_value

            with open(filename, 'w', encoding='utf-8') as file:
                json.dump(tmp_data, file, ensure_ascii=False, indent=4)
                file.close()
    except Exception as err:
        LOG.warning("Value %s (%s, %s, %s) is unknown. Returing raw value. Cause: %s",
                    value[0:100],
                    resource_name,
                    resource_ref_key,
                    attr_name,
                    err)

        return value

def flatten_array(nd_array):
    """
        Convert an n-dimensional array into a 10d array plus a shape string
        so it can be stored in a 1-d plane.
    """
    #Convert the array into a numpy array
    np_val = array(eval(repr(nd_array)))
    #Get the current shape
    shape = np_val.shape

    #Now reshape the array into a 1-d array, keepoing track of the previous
    #eimsnsions
    new_dim = 1
    shape_str = []

    for sub_dim in shape:
        new_dim = new_dim * sub_dim
        shape_str.append(str(sub_dim))

    one_dimensional_val = np_val.reshape(1, new_dim)

    flat_arr = [str(x) for x in one_dimensional_val.tolist()[0]]

    return flat_arr, shape_str
