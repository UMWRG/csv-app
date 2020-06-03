"""
    Extract the data values from datsets and return the appropriate
    string to place into the csv file
"""
import os
import json
import logging
from numpy import array
import pandas as pd

LOG = logging.getLogger(__name__)

class DataProcessor(object):
    def __init__(self, target_dir):
        self.target_dir = target_dir

        #keeps track of the ordering of column names
        self.col_names = {}

    def process_dataset(self, dataset, attribute_name, resource_name,
                        resource_ref_key):
        """
            Returns the value and metadata of a given dataset object
        """
        metadata = {}
        return_value = ''
        data_type = dataset.type.lower()

        if data_type == 'descriptor':
            return_value = self.process_descriptor(dataset.value)
        elif data_type == 'array':
            return_value = self.process_array(dataset.value,
                                              attribute_name,
                                              resource_name,
                                              resource_ref_key)
        elif data_type == 'scalar':
            return_value = self.process_scalar(dataset.value)
        elif data_type in ('dataframe', 'timeseries'):
            return_value = self.process_dataframe(dataset.value,
                                                  attribute_name,
                                                  resource_name,
                                                  resource_ref_key,
                                                  data_type=data_type)
        else:
            return_value = self.process_unknown_value(dataset.value,
                                                      attribute_name,
                                                      resource_name,
                                                      resource_ref_key,
                                                      data_type=data_type)

        if dataset is not None and dataset.metadata is not None:
            metadata = dataset.metadata

        return (str(return_value), metadata)

    def process_descriptor(self, value):
        """
            Get the raw valuye of a descriptor -- just stringify it
        """
        return str(value)

    def process_scalar(self, value):
        """
            Get the raw value of a scalar -- just return it
        """
        return value

    def process_array(self, value, attr_name, resource_name, resource_ref_key):
        """
            Get a csv-friendly value for a given array, write it to a file
            and return the name of the file.
            A csv-friendly array is a 1*n array, which means n*n arrays need
            to be flattened, storing the original shape so it can be returned
            to its original shape on import
        """
        file_name = "array_%s_%s.csv"%(resource_ref_key, attr_name)
        file_loc = os.path.join(self.target_dir, file_name)
        if os.path.exists(file_loc):
            arr_file = open(file_loc, 'a')
        else:
            arr_file = open(file_loc, 'w')

        arr_val = json.loads(value)

        flat_array, shape = self.flatten_array(arr_val)

        arr_file.write("%s,%s,%s\n" % (resource_name,
                                       ' '.join(shape),
                                       ','.join(flat_array)))

        arr_file.close()

        return file_name

    def process_dataframe(self, value, attr_name, resource_name,
                          resource_ref_key, data_type='dataframe'):
        """
            Get a csv-friendly value for a given datafra,e, write it to a file
            and return the name of the file.

            A timeseries is also a dataframe, except the index is timestamps.
        """

        if isinstance(value, str):
            value = json.loads(value)

        if value is None or value == {}:
            LOG.debug("Not exporting %s from resource %s as it is empty",
                      attr_name, resource_name)

        col_names = list(value.keys())
        file_name = "%s_%s_%s.csv"%(data_type, resource_ref_key, attr_name)
        file_loc = os.path.join(self.target_dir, file_name)
        if os.path.exists(file_loc):
            ts_file = open(file_loc, 'a')
            if self.col_names[file_name] != col_names:
                #rearrange the dataframe to match
                new_df = {}
                for colname in self.col_names[file_name]:
                    new_df[colname] = value[colname]
                value = new_df
        else:
            ts_file = open(file_loc, 'w')

            ts_file.write("name,index,shape,%s\n"%','.join(col_names))

            self.col_names[file_name] = col_names


        timestamps = value[col_names[0]].keys()
        ts_dict = {}
        for timestamp in timestamps:
            ts_dict[timestamp] = []

        for col, ts in value.items():
            for timestep, val in ts.items():
                ts_dict[timestep].append(val)

        for timestep, val in ts_dict.items():
            flat_array, shape = self.flatten_array(val)
            ts_file.write("%s,%s,%s,%s\n"%
                          (resource_name,
                           timestep,
                           ' '.join(shape),
                           ','.join(flat_array)))

        ts_file.close()

        return file_name

    def process_unknown_value(self, value, attr_name, resource_name,
                              resource_ref_key, data_type):
        """
            Process a value which is not of a standard hydra type.
        """
        processed_value = value
        try:

            #If it's json serialiseable, write it to the 'unknown data file' to preserve the data
            json_value = json.loads(value)

            #Create the folder structure to store the data
            relative_data_path = os.path.join('data', f'{resource_ref_key}')
            full_data_path = os.path.join(self.target_dir, relative_data_path)
            if not os.path.exists(full_data_path):
                os.makedirs(full_data_path)

            relative_filepath = os.path.join(relative_data_path,
                                             f'{resource_name}_{attr_name}.json')

            full_filepath = os.path.join(self.target_dir, relative_filepath)

            data_key = f"{resource_name}<>{attr_name}"
            if not os.path.exists(full_filepath):
                with open(full_filepath, 'w+', encoding='utf-8') as file:
                    LOG.warning(full_filepath)
                    file_data = {
                        data_key:{
                            'data_type':data_type,
                            'data':json_value
                            }
                        }
                    json.dump(file_data, file, ensure_ascii=False, indent=4)
                    file.close()
            else:
                LOG.info("Writing to %s", full_filepath)
                with open(full_filepath, 'r', encoding='utf-8') as file:
                    tmp_data = json.load(file)
                    tmp_data[data_key] = {
                        'data_type': data_type,
                        'data':json_value
                    }

                with open(full_filepath, 'w', encoding='utf-8') as file:
                    json.dump(tmp_data, file, ensure_ascii=False, indent=4)
                    file.close()

            processed_value = relative_filepath

        except Exception as err:
            LOG.warning("Value %s (%s, %s, %s) is unknown. Returing raw value. Cause: %s",
                        value[0:100],
                        resource_name,
                        resource_ref_key,
                        attr_name,
                        err)

        return processed_value

    def flatten_array(self, nd_array):
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
