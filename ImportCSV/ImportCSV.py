#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Copyright 2013, 2014, 2015 University of Manchester\
#\
# ImportCSV is free software: you can redistribute it and/or modify\
# it under the terms of the GNU General Public License as published by\
# the Free Software Foundation, either version 3 of the License, or\
# (at your option) any later version.\
#\
# ImportCSV is distributed in the hope that it will be useful,\
# but WITHOUT ANY WARRANTY; without even the implied warranty of\
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\
# GNU General Public License for more details.\
# \
# You should have received a copy of the GNU General Public License\
# along with ImportCSV.  If not, see <http://www.gnu.org/licenses/>\
#

"""A Hydra plug-in for importing CSV files.

Basics
~~~~~~

The plug-in for importing CSV files assumes that you have a collection of files
ready. You need one or several files for nodes (usually one file per node type)
and files for links (usually on file per link type). One node file and one link
file are mandatory, additional files are optional. The plug-in also allows you
to import network attributes.

Basic usage::

       ImportCSV.py [-h] [-p PROJECT] [-s SCENARIO] [-t NETWORK]
                    -n NODES [NODES ...] -l LINKS [LINKS ...]
                    [-r RULES [RULES ...]] [-z TIMEZONE]
                    [-g GROUPS] [-k GROUPMEMBERS]
                    [-t TEMPLATE]
                    [-u SERVER=URL] [-c SESSION-ID]
                    [-x]

Options
~~~~~~~

====================== ====== ============ =======================================
Option                 Short  Parameter    Description
====================== ====== ============ =======================================
``--help``             ``-h``              show help message and exit.
``--project``          ``-p`` PROJECT      The ID of an existing project. If no
                                           project is specified or if the ID
                                           provided does not belong to an existing
                                           project, a new one will be created.
``--scenario``         ``-s`` SCENARIO     Specify the name of the scenario
                                           created by the import function. Every
                                           import creates a new scenario. If no
                                           name is provided a default name will be
                                           assigned.
``--network``          ``-t`` NETWORK      Specify the file containing network
                                           information. If no file is specified, a
                                           new network will be created using
                                           default values.
``--network_id``       ``-i`` NETWORK_ID   Specify the ID of the network to be
                                           updated, if not specified,a new network
                                           will be created.
``--nodes``            ``-n`` NODES        One or multiple files containing nodes
                                           and attributes.
``--links``            ``-l`` LINKS        One or multiple files containing
                                           information on links.
``--groups``           ``-g`` GROUPS       A file or list of files containing
                                           group name, description,
                                           attributes and data.
``--groupmembers``     ``-k`` MEMBERS      A file or list of files containing
                                           group members.
``--template``         ``-m`` TEMPLATE     XML file defining the types for the
                                           network. Required if types are set.
``--rules``            ``-l`` RULES        File(s) containing rules or constraints
                                           as mathematical expressions.
``--timezone``         ``-z`` TIMEZONE     Specify a timezone as a string
                                           following the Area/Loctation pattern
                                           (e.g.  Europe/London). This timezone
                                           will be used for all timeseries data
                                           that is imported. If you don't specify
                                           a timezone, it defaults to UTC.
``--expand-filenames`` ``-x``              If the import function encounters
                                           something that looks like a filename,
                                           it tries to read the file.
``--server-url``       ``-u`` SERVER-URL   Url of the server the plugin will
                                           connect to.
                                           Defaults to localhost.
``--session-id``       ``-c`` SESSION-ID   Session ID used by the callig software.
                                           If left empty, the plugin will attempt
                                           to log in itself.
====================== ====== ============ =======================================


File structure
~~~~~~~~~~~~~~

In the node and link file a minimum of information has to be provided in order
to be able to import a complete network. Optionally the files can define any
number of attributes for nodes and links.

For nodes a valid file looks like this::

    Name , x, y, type, attribute_1, attribute_2, ..., attribute_n, description
    Units,  ,  ,     ,           m,    m^3 s^-1, ...,           -,
    node1, 2, 1, irr ,         4.0,      3421.9, ...,  Crop: corn, Irrigation 1
    node2, 2, 3, irr ,         2.4,       988.4, ...,  Crop: rice, Irrigation 2

For links, the following is a valid file::

    Name ,       from,       to, type, attre_1, ...,  attre_n, description
    Units,           ,         ,     ,       m, ..., m^2 s^-1,
    link1,      node1,    node2, tran,     453, ...,     0.34, Water transfer

It is optional to supply a network file. If you decide to do so, it needs to
follow this structure::

    # A test network created as a set of CSV files
    ID, Name            , type, attribute_1, ..., Description
    Units,              ,     ,            ,    ,
    1 , My first network, net , test       ,    , Network created from CSV files


Constraint groups come in 2 files.
The first file defines the name, description and attributes of a file and looks like this::

    Name  , attribute_1, attribute_2..., Description
    Units , hm^3       , m             ,
    stor  , totalCap   , maxSize       , Storage nodes
    ...   , ...        , ...           , ...

The second file defines the members of the groups.
The group name, the type of the member (node, link or another group) and the name
of that other member are needed::

    Name  , Type  , Member
    stor  , NODE  , node1
    stor  , NODE  , node2
    stor  , LINK  , link1

Metadata can also be included in separate files, which are **named the same
as the node/link file, but with _metadata at the end.**

For example:
    nodes.csv becomes nodes_metadata.csv
    network.csv becomes network_metadata.csv
    my_urban_links.csv becomes my_urban_links_metadata.csv


Metadata files are structured as follows:
    
 Name  , attribute_1             , attribute_2             , attribute_3
 link1 , (key1:val1) (key2:val2) , (key3:val3) (key4:val4) , (key5:val5)

In this case, key1 and key2 are metadata items for attribute 1 and so on.
The deliminator for the key-val can be ';' or ':'. Note that all key-val
pairs are contained within '(...)', with a space between each one. This way
you can have several metadata items per attribute.

Lines starting with the ``#`` character are ignored.

.. note::

   If you specify a header line using the keywords ``name``, ``x``, ``y`` and
   ``description`` (``name``, ``from``, ``to`` and ``description`` for links)
   the order of the columns does not matter. If you don't specify these
   keywords, the plug-in will assume that the first column specifies the name,
   the second X, the third Y and the last the description (name, from, to and
   description for links).

.. note::

    The ``type`` column is optional.

Please also consider the following:

- A link references to start and end node by name. This also implies that the\
  node names defined in the file need to be unique.
- The description should be in the last column. This will lead to more readable\
  files, since the description is usually free form text.
- If you specify more than one file containing nodes, common attributes (i.e.\
  with the same name) will be considered the same. This results in a unique\
  attribute set for nodes. The same applies for links.
- An attribute to a node or link is only added if there is a value for that\
  specific attribute in the line specifying a node (or link). If an attribute\
  should be added as a variable, you need to enter ``NULL``.
- If you use a tmplate during import, missing attributes will be added to each\
  node or link according to its type.

TODO
----

- Implement updating of existing scenario.

Building a windows executable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 - Use pyinstaller (pip install pyisntaller) to build a windows executable.
 - cd to the $PATH_TO_HYDRA/HydraPlugins/CSVPlugin/trunk
 - pyinstaller -F ImportCSV.py
 - If you want more compression (a smaller exe), install upx and run pyinstaller -F --upx-dir=/path/to/upx/dir ExportCSV.py
 - An executable file will appear in the dist folder

API docs
~~~~~~~~
"""

import argparse as ap
import logging
import os, sys
from datetime import datetime
import pytz

from HydraLib import PluginLib
from HydraLib.PluginLib import JsonConnection, write_progress, write_output, validate_plugin_xml, RequestError, validate_template

from HydraLib.units import validate_resource_attributes

from HydraLib.HydraException import HydraPluginError

from csv_util import get_file_data, check_header, parse_unit
from rules import RuleReader

from data import create_dataset

log = logging.getLogger(__name__)

__location__ = os.path.split(sys.argv[0])[0]

class ImportCSV(object):
    """
    """

    def __init__(self, url=None, session_id=None):

        self.url = url

        self.Project  = None
        self.Network  = None
        self.NetworkSummary  = None
        self.Scenario = None
        self.Nodes    = dict()
        self.Links    = dict()
        self.Groups   = dict()
        self.Attributes = dict()
        self.Rules      = dict()

        #Store the names of the input files here. Taken from the network file.
        self.node_args = []
        self.link_args = []
        self.group_args = []
        self.groupmember_args = []
        self.rule_args = []

        #This stores all the types in the template
        #so that node, link and group types can be validated
        self.Templates      = dict()

        #These are used to keep track of whether
        #duplicate names have been specified in the files.
        self.link_names = []
        self.node_names = []
        self.group_names = []

        self.update_network_flag = False
        self.timezone = pytz.utc
        self.expand_filenames = False
        self.file_dict = {}
        self.basepath = ''

        self.add_attrs = True
        self.nodetype_dict = dict()
        self.linktype_dict = dict()
        self.grouptype_dict = dict()
        self.networktype = ''

        self.connection = JsonConnection(url)
        if session_id is not None:
            log.info("Using existing session %s", session_id)
            self.connection.session_id=session_id
        else:
            self.connection.login()

        self.node_id  = PluginLib.temp_ids()
        self.link_id  = PluginLib.temp_ids()
        self.group_id = PluginLib.temp_ids()
        self.attr_id  = PluginLib.temp_ids()

        self.units = self.get_dimensions()

        self.warnings = []
        self.message = ''
        self.files = []

        self.num_steps = 9

        self.ignorelines = ['', '\n', '\r']



    def get_dimensions(self):
        units = {}
        dimensions = self.connection.call('get_all_dimensions', {})
        for dimension in dimensions:
            for unit_name in dimension['units']:
                units[unit_name] = dimension['name']
        return units

    def create_project(self, ID=None, network_id=None):
        if ID is not None:
            try:
                ID = int(ID)
                try:
                    self.Project = self.connection.call('get_project', {'project_id':ID})
                    networks = self.connection.call('get_networks', {'project_id':ID,
                                                                     'include_data':'N'})
                    self.Project['networks'] = networks
                    log.info('Loading existing project (ID=%s)' % ID)
                    return
                except RequestError:
                    log.info('Project ID not found. Creating new project')

            except ValueError:
                log.info('Project ID not valid. Creating new project')
                self.warnings.append(
                    'Project ID not valid. Creating new project')
        elif network_id is not None:
            try:
                network_id = int(network_id)
                try:
                    self.Project = self.connection.call('get_network_project', {'network_id':network_id})
                    log.info('Loading existing project with network ID(ID=%s)' % network_id)
                    return
                except RequestError:
                    log.info('Project ID not found. Creating new project')

            except ValueError:
                log.info('Project ID not valid. Creating new project')
                self.warnings.append(
                    'Project ID not valid. Creating new project')

        self.Project = dict(
            name = "CSV import at %s" % (datetime.now()),
            description = \
            "Project created by the %s plug-in, %s." % \
                (self.__class__.__name__, datetime.now()),
            status = 'A',
        )
        self.Project = self.connection.call('add_project', {'project':self.Project})
        self.Project['networks']=[]

    def create_scenario(self, name=None):

        self.Scenario = dict()
        if name is not None:
            self.Scenario['name'] = name
        else:
            self.Scenario['name'] = 'CSV import'

        self.Scenario['description']= \
            'Default scenario created by the CSV import plug-in.'
        self.Scenario['id'] = -1
        self.Scenario['resourcescenarios'] = []

    def create_network(self, file=None, network_id=None):

        if file is not None:

            self.basepath = os.path.dirname(os.path.realpath(file))
            
            net_data = get_file_data(file)

            try:
                file_parts = file.split(".")
                file_base = file_parts[0]
                file_ext = file_parts[1]
                new_filename = "%s_metadata.%s"%(file_base, file_ext)
                metadata = self.read_metadata(new_filename)
            except IOError:
                log.info("No metadata found for node file %s",file)
                metadata = {}

            keys = [k.strip() for k in net_data[0].split(',')]
            #Make a list of all the keys in lowercase so we can perform
            #checks later on for certain headings.
            lower_keys = [k.lower() for k in keys]

            check_header(file, keys)
            if net_data[1].lower().startswith('unit'):
                units = net_data[1].split(',')
                data_idx = 2
            else:
                units = None
                data_idx = 1
            # A network file should only have one line of data
            data = net_data[data_idx].split(',')
            if units is not None:
                for i, unit in enumerate(units):
                    units[i] = unit.strip()

            # We assume a standard order of the network information (Name,
            # Description, attributes,...).
            field_idx = {'id': 0,
                         'name': 1,
                         'description': -1,
                         'projection':3,
                         'type': 2,
                         'nodes':3,
                         'links':4,
                         'groups':5,
                         'rules':6,
                         }
            # If the file does not follow the standard, we can at least try to
            # guess what is stored where.
            for i, key in enumerate(keys):
                if key.lower().strip() in field_idx.keys():
                    field_idx[key.lower().strip()] = i

            if field_idx['type'] is not None:
                self.networktype = data[field_idx['type']].strip()
        
            #Identify the node, link and group files to import
            #Remove trailing white space and trailing ';', which can cause issues.
            self.node_args  = data[field_idx['nodes']].strip().strip(';').split(';')
            if 'links' in lower_keys: 
                self.link_args  = data[field_idx['links']].strip().strip(';').split(';')
            if 'groups' in lower_keys:
                self.group_args = data[field_idx['groups']].strip().strip(';').split(';')
            if 'rules' in lower_keys:
                self.rule_args  = data[field_idx['rules']].strip().strip(';').split(';')

            if network_id is not None:
                # Check if network exists on the server.
                try:
                    self.Network = \
                            self.connection.call('get_network', {'network_id':int(network_id), 'include_data':'N', 'summary':'N'})

                    if self.Scenario['name'] in [s['name'] for s in self.Network['scenarios']]:
                        raise HydraPluginError("Network already has a scenario called %s. Chooses another scenario name for this network."%(self.Scenario['name'],))

                    # Assign name and description in case anything has changed
                    self.Network['name'] = data[field_idx['name']].strip()
                    self.Network['description'] = \
                        data[field_idx['description']].strip()
                    self.Network['projection'] = \
                        data[field_idx['projection']].strip()
                    self.update_network_flag = True
                    log.info('Loading existing network (ID=%s)' % network_id)
                    # load existing nodes
                    for node in self.Network['nodes']:
                        self.Nodes.update({node['name']: node})
                    # load existing links
                    for link in self.Network['links']:
                        self.Links.update({link['name']: link})
                    # load existing groups
                    for group in self.Network['resourcegroups']:
                        self.Groups.update({group['name']: group})

                    # Nodes and links are now deleted from the network, they
                    # will be added later...
                    self.Network['nodes']  = []
                    self.Network['links']  = []
                    self.Network['resourcegroups'] = []
                    # The scenario loaded with the network will be deleted as
                    # well, we create a new one.
                    self.Network['scenarios'] = []
                    self.Network['type'] = data[field_idx['type']].strip()
                except RequestError:
                    log.info('Network %s not found. Creating new network.', network_id)
                    self.warnings.append('Network %s not found. Creating new network.'%(network_id,))
                    network_id = None

            if network_id is None:

                network_name = data[field_idx['name']].strip()
                for n in self.Project['networks']:
                    if network_name == n['name']:
                        raise HydraPluginError("Project %s already has a network called %s"%(self.Project['name'], network_name))

                # Create a new network
                self.Network = dict(
                    project_id = self.Project['id'],
                    name = network_name,
                    description = \
                    data[field_idx['description']].strip(),
                    projection = data[field_idx['projection']].strip(),
                    nodes = [],
                    links = [],
                    scenarios = [],
                    resourcegroups = [],
                    attributes = [],
                    type=data[field_idx['type']].strip(),
                )

            # Everything that is not name or description is an attribute
            attrs = dict()

            for i, key in enumerate(keys):
                if i not in field_idx.values():
                    attrs.update({i: key.strip()})

            if len(attrs.keys()) > 0:
                self.Network = self.add_data(self.Network, attrs, data, metadata, units=units)

        else:
            # Create a new network
            self.Network = dict(
                project_id = self.Project['id'],
                name = "CSV import",
                description = \
                "Network created by the %s plug-in, %s." % \
                (self.__class__.__name__, datetime.now()),
                projection = "",
                nodes = [],
                links = [],
                scenarios = [],
                resourcegroups = [],
                attributes = [],
            )

    def read_metadata(self, filename):
        log.info("Reading metadata from file %s", filename)
        metadata = get_file_data(filename)
        keys = metadata[0].split(',')
        check_header(filename, keys)
        data = metadata[1:]

        metadata_dict = {}
        for line_num, data_line in enumerate(data):
            try:
                data_line = data_line.replace('\r', '')
                split_data = data_line.split(',')
                metadata_dict[split_data[0].strip()] = self.get_metadata_as_dict(keys[1:], split_data[1:])
            except Exception, e:
                raise HydraPluginError("Malformed metadata for %s, line %s of %s"%(e.message, line_num+2, filename))
        return metadata_dict

    def get_metadata_as_dict(self, keys, metadata):
        """
            Turn a list of metadata values into a dictionary structure.
            @parameter keys to describe the attribte to which this metadata refers
            @parameter list of metadata. in the structure: ["(key;val) (key;val)", "(key;val) (key;val)",...]
            @returns dictionary in the format: {attr1 : {key:val, key:val}, attr2: {key:val, key:val}...}
        """
        metadata_dict = {}
        for i, attr in enumerate(keys):
            try:
                metadata_dict[attr.strip()] = {}
                if metadata[i].strip() != '':
                    attr_metadata = metadata[i].split(")")
                    for attr_meta in attr_metadata:
                        if attr_meta == '':
                            continue
                        attr_meta = attr_meta.replace('(', '')
                        #Check if it's ';' or ':' that is the deliminator..
                        if attr_meta.find(';') > 0:
                            keyval = attr_meta.split(';')
                        else:
                            keyval = attr_meta.split(':')

                        key = keyval[0].strip()
                        if key.lower() in ('name', 'dataset_name', 'dataset name'):
                            key = 'name'
                        val = keyval[1].strip()
                        metadata_dict[attr.strip()][key] = val
            except Exception, e:
                log.critical(e)
                log.critical("Make sure the CSV file is formatted correctly.")
                raise Exception(attr)

        return metadata_dict

    def read_nodes(self, file):
        node_data = get_file_data(file)

        try:
            file_parts = file.split(".")
            file_base = file_parts[0]
            file_ext = file_parts[1]
            new_filename = "%s_metadata.%s"%(file_base, file_ext)
            metadata = self.read_metadata(new_filename)
        except IOError:
            log.info("No metadata found for node file %s",file)
            metadata = {}

        self.add_attrs = True

        keys  = node_data[0].split(',')
        check_header(file, keys)

        #There may or may not be a units line, so we need to account for that.
        if node_data[1].lower().startswith('unit'):
            units = node_data[1].split(',')
            for i, unit in enumerate(units):
                units[i] = unit.strip()

            data_idx = 2
        else:
            units = None
            data_idx = 1
        # Get all the lines after the units line. 
        data = node_data[data_idx:]

        field_idx = {'name': 0,
                     'description': -1,
                     'x': 1,
                     'y': 2,
                     'type': None,
                     }
        # Guess parameter position:
        attrs = dict()
        for i, key in enumerate(keys):

            if key.lower().strip() in field_idx.keys():
                field_idx[key.lower().strip()] = i
            else:
                attrs.update({i: key.strip()})

        for line_num, line in enumerate(data):

            #skip any empty lines
            if line.strip() in self.ignorelines:
                continue

            try:
                node = self.read_node_line(line, attrs, field_idx, metadata, units)
            except Exception, e:
                log.exception(e)
                raise HydraPluginError("An error has occurred in file %s at line %s: %s"%(os.path.split(file)[-1], line_num+3, e))

            self.Nodes.update({node['name']: node})

    def read_node_line(self, line, attrs, field_idx, metadata, units):
        linedata = line.split(',')
        nodename = linedata[field_idx['name']].strip()

        restrictions = {}
        
        if nodename in self.node_names:
            raise HydraPluginError("Duplicate Node name: %s"%(nodename))
        else:
            self.node_names.append(nodename)

        if nodename in self.Nodes.keys():
            node = self.Nodes[nodename]
            log.debug('Node %s exists.' % nodename)
        else:
            node = dict(
                id = self.node_id.next(),
                name = nodename,
                description = linedata[field_idx['description']].strip(),
                attributes = [],
            )
        try:
            float(linedata[field_idx['x']].strip())
            node['x'] = linedata[field_idx['x']].strip()
        except ValueError:
            node['x'] = None
            log.info('X coordinate of node %s is not a number.'
                         % node['name'])
            self.warnings.append('X coordinate of node %s is not a number.'
                                 % node['name'])
        try:
            float(linedata[field_idx['y']].strip())
            node['y'] = linedata[field_idx['y']].strip()
        except ValueError:
            node['y'] = None
            log.info('Y coordinate of node %s is not a number.'
                         % node['name'])
            self.warnings.append('Y coordinate of node %s is not a number.'
                                 % node['name'])
        if field_idx['type'] is not None:
            node_type = linedata[field_idx['type']].strip()
            node['type'] = node_type

            if len(self.Templates):
                if node_type not in self.Templates['resources'].get('NODE', {}).keys():
                    raise HydraPluginError(
                        "Node type %s not specified in the template."%
                        (node_type))
            
                restrictions = self.Templates['resources']['NODE'][node_type]['attributes']
            if node_type not in self.nodetype_dict.keys():
                self.nodetype_dict.update({node_type: (nodename,)})
            else:
                self.nodetype_dict[node_type] += (nodename,)

        if len(attrs) > 0:
            node = self.add_data(node, attrs, linedata, metadata, units=units, restrictions=restrictions)

        return node


    def read_links(self, file):

        if file == "":
            self.warnings.append("No links specified")
            return

        link_data = get_file_data(file)

        try:
            file_parts = file.split(".")
            file_base = file_parts[0]
            file_ext = file_parts[1]
            new_filename = "%s_metadata.%s"%(file_base, file_ext)
            metadata = self.read_metadata(new_filename)
        except IOError:
            log.info("No metadata found for node file %s",file)
            metadata = {}

        self.add_attrs = True

        keys = link_data[0].split(',')
        check_header(file, keys)

        #There may or may not be a units line, so we need to account for that.
        if link_data[1].lower().startswith('unit'):
            units = link_data[1].split(',')
            for i, unit in enumerate(units):
                units[i] = unit.strip()

            data_idx = 2
        else:
            units = None
            data_idx = 1

        # Get all the lines after the units line 
        data = link_data[data_idx:]

        field_idx = {'name': 0,
                     'description': -1,
                     'from': 1,
                     'to': 2,
                     'type': None,
                     }
        # Guess parameter position:
        attrs = dict()
        for i, key in enumerate(keys):
            if key.lower().strip() in field_idx.keys():
                field_idx[key.lower().strip()] = i
            else:
                attrs.update({i: key.strip()})

        for line_num, line in enumerate(data):
            #skip any empty lines
            if line.strip() in self.ignorelines:
                continue

            try:
                link = self.read_link_line(line, attrs, field_idx, metadata, units)
            except Exception, e:
                log.exception(e)
                raise HydraPluginError("An error has occurred in file %s at line %s: %s"%(os.path.split(file)[-1], line_num+3, e))

            if link is not None:
                self.Links.update({link['name']: link})

    def read_link_line(self, line, attrs, field_idx, metadata, units):

        restrictions = {}
        linedata = line.split(',')
        linkname = linedata[field_idx['name']].strip()

        if linkname in self.link_names:
            raise HydraPluginError("Duplicate Link name: %s"%(linkname))
        else:
            self.link_names.append(linkname)

        if linkname in self.Links.keys():
            link = self.Links[linkname]
            log.debug('Link %s exists.' % linkname)
        else:
            link = dict(
                id = self.link_id.next(),
                name = linkname,
                description = linedata[field_idx['description']].strip(),
                attributes = [], 
            )

        try:
            fromnode = self.Nodes[linedata[field_idx['from']].strip()]
            tonode = self.Nodes[linedata[field_idx['to']].strip()]
            link['node_1_id'] = fromnode['id']
            link['node_2_id'] = tonode['id']

        except KeyError:
            log.info(('Start or end node not found (%s -- %s).' +
                          ' No link created.') %
                         (linedata[field_idx['from']].strip(),
                          linedata[field_idx['to']].strip()))
            self.warnings.append(('Start or end node not found (%s -- %s).' +
                          ' No link created.') %
                         (linedata[field_idx['from']].strip(),
                          linedata[field_idx['to']].strip()))
            return None

        if field_idx['type'] is not None:
            link_type = linedata[field_idx['type']].strip()
            link['type'] = link_type
            if len(self.Templates):
                if link_type not in self.Templates['resources'].get('LINK', {}).keys():
                    raise HydraPluginError(
                        "Link type %s not specified in the template."
                        %(link_type))
            
                restrictions = self.Templates['resources']['LINK'][link_type]['attributes']
            if link_type not in self.linktype_dict.keys():
                self.linktype_dict.update({link_type: (linkname,)})
            else:
                self.linktype_dict[link_type] += (linkname,)
        if len(attrs) > 0:
            link = self.add_data(link, attrs, linedata, metadata, units=units, restrictions=restrictions)
        
        return link

    def read_groups(self, file):
        """
            The heading of a group file looks like:
            name, attr1, attr2..., description
        """

        if file == "":
            self.warnings.append("No groups specified")
            return

        group_data = get_file_data(file)

        try:
            file_parts = file.split(".")
            file_base = file_parts[0]
            file_ext = file_parts[1]
            new_filename = "%s_metadata.%s"%(file_base, file_ext)
            metadata = self.read_metadata(new_filename)
        except IOError:
            log.info("No metadata found for node file %s",file)
            metadata = {}

        self.add_attrs = True

        keys  = group_data[0].split(',')
        check_header(file, keys)

        #There may or may not be a units line, so we need to account for that.
        if group_data[1].lower().startswith('unit'):
            units = group_data[1].split(',')
            for i, unit in enumerate(units):
                units[i] = unit.strip()

            data_idx = 2
        else:
            units = None
            data_idx = 1

        # Get all the lines after the units line
        data = group_data[data_idx:]

        #Indicates what the mandatory columns are and where
        #we expect to see them.
        field_idx = {'name': 0,
                     'description': -1,
                     'type': None,
                     'members':2,
                     }

        attrs = dict()
        # Guess parameter position:
        for i, key in enumerate(keys):
            if key.lower().strip() in field_idx.keys():
                field_idx[key.lower().strip()] = i
            else:
                attrs.update({i: key.strip()})

        for line_num, line in enumerate(data):

            #skip any empty lines
            if line.strip() in self.ignorelines:
                continue
            try: 
                group = self.read_group_line(line, attrs, field_idx, metadata, units)
            except Exception, e:
                log.exception(e)
                raise HydraPluginError("An error has occurred in file %s at line %s: %s"%(os.path.split(file)[-1], line_num+3, e))
           
            self.Groups.update({group['name']: group})

    def read_group_line(self, line, attrs, field_idx, metadata, units):

        group_data = line.split(',')
        group_name = group_data[field_idx['name']].strip()

        member_file = group_data[field_idx['members']].strip()
        if member_file not in self.groupmember_args:
            self.groupmember_args.append(member_file)

        restrictions = {}

        if group_name in self.group_names:
            raise HydraPluginError("Duplicate Group name: %s"%(group_name))
        else:
            self.group_names.append(group_name)

        if group_name in self.Groups.keys():
            group = self.Groups[group_name]
            log.debug('Group %s exists.' % group_name)
        else:
            group = dict(
                id = self.group_id.next(),
                name = group_name,
                description = group_data[field_idx['description']].strip(),
                attributes = [],
            )

        if field_idx['type'] is not None:

            group_type = group_data[field_idx['type']].strip()
            group['type'] = group_type

            if len(self.Templates):
                if group_type not in self.Templates['resources'].get('GROUP', {}).keys():
                    raise HydraPluginError(
                        "Group type %s not specified in the template."
                        %(group_type))
                restrictions = self.Templates['resources']['GROUP'][group_type]['attributes']

            if group_type not in self.grouptype_dict.keys():
                self.grouptype_dict.update({group_type: (group_name,)})
            else:
                self.grouptype_dict[group_type] += (group_name,)


        if len(attrs) > 0:
            group = self.add_data(group, attrs, group_data, metadata, units=units, restrictions=restrictions)

        return group


    def read_group_members(self, file):

        """
            The heading of a group file looks like:
            name, type, member.

            name : Name of the group
            type : Type of the member (node, link or group)
            member: name of the node, link or group in question.

        """
        member_data = get_file_data(file)

        keys  = member_data[0].split(',')
        check_header(file, keys)
        #There may or may not be a units line, so we need to account for that.
        if member_data[1].lower().startswith('unit'):
            units = member_data[1].split(',')
            for i, unit in enumerate(units):
                units[i] = unit.strip()

            data_idx = 2
        else:
            units = None
            data_idx = 1

        # Get all the lines after the units line 
        data = member_data[data_idx:]

        field_idx = {}
        for i, k in enumerate(keys):
            field_idx[k.lower().strip()] = i

        type_map = {
                'NODE' : self.Nodes,
                'LINK' : self.Links,
                'GROUP': self.Groups,
            }

        items = []

        for line_num, line in enumerate(data):

            #skip any empty lines
            if line.strip() in self.ignorelines:
                continue

            try:
                item = self.read_group_member_line(line, field_idx, type_map)
                if item is None:
                    continue
            except Exception, e:
                log.exception(e)
                raise HydraPluginError("An error has occurred in file %s at line %s: %s"%(os.path.split(file)[-1], line_num+3, e))


            items.append(item)

        self.Scenario['resourcegroupitems'] = items

    def read_group_member_line(self, line, field_idx, type_map):

        member_data = line.split(',')
        group_name  = member_data[field_idx['name']].strip()
        group = self.Groups.get(group_name)

        if group is None:
            log.info("Group %s has not been specified."%(group_name) +
                      ' Group item not created.')
            self.warnings.append("Group %s has not been specified"%(group_name) +
                      ' Group item not created.')
            return None


        member_type = member_data[field_idx['type']].strip().upper()

        if type_map.get(member_type) is None:
            log.info("Type %s does not exist."%(member_type) +
                      ' Group item not created.')
            self.warnings.append("Type %s does not exist"%(member_type) +
                      ' Group item not created.')
            return None
        member_name = member_data[field_idx['member']].strip()

        member = type_map[member_type].get(member_name)
        if member is None:
            log.info("%s %s does not exist."%(member_type, member_name) +
                      ' Group item not created.')
            self.warnings.append("%s %s does not exist."%(member_type, member_name) +
                      ' Group item not created.')
            return None

        item = dict(
            group_id = group['id'],
            ref_id   = member['id'],
            ref_key  = member_type,
        )

        return item

    
    def create_attribute(self, name, unit=None):
        """
            Create attribute locally. It will get added in bulk later.
        """
        try:
            attribute = dict(
                name = name.strip(),
            )
            if unit is not None and len(unit.strip()) > 0:
                #Unit added to attribute definition for validation only. Not saved in DB
                attribute['unit'] = unit.strip()
                #Dimension is saved in DB.
                if unit.strip() not in ('-' ,''):
                    basic_unit, factor = parse_unit(unit.strip())
                    attribute['dimen'] = self.units.get(basic_unit)

        except Exception,e:
            raise HydraPluginError("Invalid attribute %s %s: error was: %s"%(name,unit,e))

        return attribute

    def add_data(self, resource, attrs, data, metadata, units=None, restrictions={}):
        '''Add the data read for each resource to the resource. This requires
        creating the attributes, resource attributes and a scenario which holds
        the data.'''

        attributes = []

        # Collect existing resource attributes:
        resource_attrs = dict()

        if resource.get('attributes') is None:
            return resource

        for res_attr in resource['attributes']:
            resource_attrs.update({res_attr.attr_id: res_attr})

        for i in attrs.keys():
            if attrs[i] in self.Attributes.keys():
                attribute = self.Attributes[attrs[i]]
                if units is not None:
                    if attribute.get('unit', '') != units[i]:
                        raise HydraPluginError("Mismatch of units for attribute %s."
                              " Elsewhere units are defined with unit %s, but here units "
                              "are %s"%(attrs[i], attribute.get('unit'), units[i]))
                #attribute = self.create_attribute(attrs[i])
            else:
                if units is not None:
                    attribute = self.create_attribute(attrs[i], units[i])
                else:
                    attribute = self.create_attribute(attrs[i])
                self.Attributes.update({attrs[i]: attribute})
            attributes.append(attribute)

        # Add all attributes. If they exist already, we retrieve the real id.
        # Also, we add the attributes only once (that's why we use the
        # add_attrs flag).

        if self.add_attrs:
            log.info(attributes)
            attributes = self.connection.call('add_attributes', {'attrs':attributes})
            self.add_attrs = False
            for attr in attributes:
                self.Attributes[attr['name']]['id'] = attr['id']

        # Add data to each attribute
        for i in attrs.keys():
            attr = self.Attributes[attrs[i]]
            # Attribute might already exist for resource, use it if it does
            if attr['id'] in resource_attrs.keys():
                res_attr = resource_attrs[attr['id']]
            else:
                res_attr = dict(
                    id = self.attr_id.next(),
                    attr_id = attr['id'],
                    attr_is_var = 'N',
                )
            # create dataset and assign to attribute (if not empty)
            if len(data[i].strip()) > 0:

                resource['attributes'].append(res_attr)

                if data[i].strip() in ('NULL',
                                       'I AM NOT A NUMBER! I AM A FREE MAN!'):

                    res_attr['attr_is_var'] = 'Y'

                elif data[i].strip() == '-':
                    continue
                else:
                    if metadata:
                        resource_metadata = metadata.get(resource['name'], {})
                        dataset_metadata = resource_metadata.get(attrs[i], {})
                    else:
                        dataset_metadata = {}
                        
                    dataset_unit = None
                    dataset_dimension = None
                    if units is not None:
                        if units[i] is not None and len(units[i].strip()) > 0 and units[i].strip() != '-':
                            dimension = attr.get('dimen')
                            if dimension is None:
                                log.debug("Dimension for unit %s is null. ", units[i])
                        else:
                            dimension = None

                        dataset_dimension = dimension
                        dataset_unit      = units[i]

                    try:
                        dataset = create_dataset(data[i],
                                                  res_attr,
                                                  dataset_unit,
                                                  dataset_dimension,
                                                  resource['name'],
                                                  dataset_metadata,
                                                  restrictions.get(attr['name'], {}).get('restrictions', {}),
                                             self.expand_filenames,
                                             self.basepath,
                                             self.file_dict,
                                             self.Scenario['name'],
                                                 self.timezone
                                                 )
                    except HydraPluginError, e:
                        self.warn(e)
                        self.warnings.extend(e)

                    if dataset is not None:
                        self.Scenario['resourcescenarios'].append(dataset)
        errors = [] 
        if len(self.Templates):
            errors = validate_resource_attributes(resource, self.Attributes, self.Templates)
        #resource.attributes = res_attr_array

        if len(errors) > 0:
            raise HydraPluginError("Errors validating resource: %s"% errors)

        return resource

    def set_resource_types(self, template_file):
        log.info("Setting resource types based on %s." % template_file)
        with open(template_file) as f:
            xml_template = f.read()

        template = self.connection.call('upload_template_xml', {'template_xml':xml_template})

        type_ids = dict()
        warnings = []

        for type_name in self.nodetype_dict.keys():
            for tmpltype in template.get('types', []):
                if tmpltype['name'] == type_name:
                    type_ids.update({tmpltype['name']: tmpltype['id']})
                    break

        for type_name in self.linktype_dict.keys():
            for tmpltype in template.get('types', []):
                if tmpltype['name'] == type_name:
                    type_ids.update({tmpltype['name']: tmpltype['id']})
                    break

        for type_name in self.grouptype_dict.keys():
            for tmpltype in template.get('types', []):
                if tmpltype['name'] == type_name:
                    type_ids.update({tmpltype['name']: tmpltype['id']})
                    break

        for tmpltype in template.get('types', []):
            if tmpltype['name'] == self.networktype:
                type_ids.update({tmpltype['name']: tmpltype['id']})
                break

        args = []

        if self.networktype == '':
            warnings.append("No network type specified")
        elif type_ids.get(self.networktype):
            args.append(dict(
                ref_key = 'NETWORK',
                ref_id  = self.NetworkSummary['id'],
                type_id = type_ids[self.networktype],
            ))
        else:
            warnings.append("Network type %s not found"%(self.networktype))

        if self.NetworkSummary.get('nodes', []):
            for node in self.NetworkSummary['nodes']:
                for typename, node_name_list in self.nodetype_dict.items():
                    if type_ids[typename] and node['name'] in node_name_list:
                        args.append(dict(
                            ref_key = 'NODE',
                            ref_id  = node['id'],
                            type_id = type_ids[typename],
                        ))
        else:
            warnings.append("No nodes found when setting template types")

        if self.NetworkSummary.get('links', []):
            for link in self.NetworkSummary['links']:
                for typename, link_name_list in self.linktype_dict.items():
                    if type_ids[typename] and link['name'] in link_name_list:
                        args.append(dict(
                            ref_key = 'LINK',
                            ref_id  = link['id'],
                            type_id = type_ids[typename],
                        ))
        else:
           warnings.append("No links found when setting template types")

        if self.NetworkSummary.get('resourcegroups'):
            for group in self.NetworkSummary['resourcegroups']:
                for typename, group_name_list in self.grouptype_dict.items():
                    if type_ids[typename] and group['name'] in group_name_list:
                        args.append(dict(
                            ref_key = 'GROUP',
                            ref_id  = group['id'],
                            type_id = type_ids[typename],
                        ))
        else:
           warnings.append("No resourcegroups found when setting template types")
        self.connection.call('assign_types_to_resources', {'resource_types':args})
        return warnings




    def commit(self):
        log.info("Committing Network")
        for node in self.Nodes.values():
            self.Network['nodes'].append(node)
        for link in self.Links.values():
            self.Network['links'].append(link)
        for group in self.Groups.values():
            self.Network['resourcegroups'].append(group)
        self.Network['scenarios'].append(self.Scenario)
        log.info("Network created for sending")

        if self.update_network_flag:
            self.NetworkSummary = self.connection.call('update_network', {'net':self.Network})
            log.info("Network %s updated.", self.Network['id'])
        else:
            log.info("Adding Network")
            self.NetworkSummary = self.connection.call('add_network', {'net':self.Network})
            log.info("Network created with %s nodes and %s links. Network ID is %s",
                     len(self.NetworkSummary['nodes']),
                     len(self.NetworkSummary['links']),
                     self.NetworkSummary['id'])

        self.message = 'Data import was successful.'

    def return_xml(self):
        """This is a fist version of a possible XML output.
        """
        scen_ids = [s['id'] for s in self.NetworkSummary['scenarios']]

        xml_response = PluginLib.create_xml_response('ImportCSV',
                                                     self.Network['id'],
                                                     scen_ids)

        print xml_response

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a network saved in a set of CSV files into Hydra.

        Written by Philipp Meier <philipp@diemeiers.ch> and 
        Stephen Knox <stephen.knox@manchester.ac.uk>
        (c) Copyright 2013, University of Manchester.

        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project',
                        help='''The ID of an existing project. If no project is
                        specified or if the ID provided does not belong to an
                        existing project, a new one will be created.''')
    parser.add_argument('-s', '--scenario',
                        help='''Specify the name of the scenario created by the
                        import function. Every import creates a new scenario.
                        If no name is provided a default name will be assigned.
                        ''')
    parser.add_argument('-t', '--network',
                        help='''Specify the file containing network
                        information. If no file is specified, a new network
                        will be created using default values.''')
    parser.add_argument('-i', '--network_id',
                        help='''The ID of an existing network. If specified,
                        this network will be updated. If not, a new network
                        will be created.
                        on links.''')
    parser.add_argument('-m', '--template',
                        help='''Template XML file, needed if node and link
                        types are specified,''')
    parser.add_argument('-r', '--rules', nargs='+',
                        help='''File(s) containing rules or constraints as
                        mathematical expressions.''')
    parser.add_argument('-z', '--timezone',
                        help='''Specify a timezone as a string following the
                        Area/Location pattern (e.g. Europe/London). This
                        timezone will be used for all timeseries data that is
                        imported. If you don't specify a timezone, it defaults
                        to UTC.''')
    parser.add_argument('-x', '--expand-filenames', action='store_true',
                        help='''If the import function encounters something
                        that looks like a filename, it tries to read the file.
                        It also tries to guess if it contains a number, a
                        descriptor, an array or a time series.''')
    parser.add_argument('-u', '--server_url',
                        help='''Specify the URL of the server to which this
                        plug-in connects.''')
    parser.add_argument('-c', '--session_id',
                        help='''Session ID. If this does not exist, a login will be
                        attempted based on details in config.''')
    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()
    csv = ImportCSV(url=args.server_url, session_id=args.session_id)

    network_id = None
    scen_ids = []
    errors = []
    try:

        write_progress(1,csv.num_steps)
        validate_plugin_xml(os.path.join(__location__, 'plugin.xml'))

        if args.expand_filenames:
            csv.expand_filenames = True

        if args.timezone is not None:
            csv.timezone = pytz.timezone(args.timezone)

        if args.template is not None:
            try:
                if args.template == '':
                    raise Exception("The template name is empty.")
                csv.Template = validate_template(args.template, csv.connection)
            except Exception, e:
                log.exception(e)
                raise HydraPluginError("An error has occurred with the template. (%s)"%(e))

        # Create project and network only when there is actual data to
        # import.
        write_progress(2,csv.num_steps)
        csv.create_project(ID=args.project, network_id=args.network_id)
        csv.create_scenario(name=args.scenario)
        csv.create_network(file=args.network, network_id=args.network_id)
        
        write_progress(3,csv.num_steps)
        for nodefile in csv.node_args:
            write_output("Reading Node file %s" % nodefile)
            csv.read_nodes(nodefile)

        write_progress(4,csv.num_steps)
        if len(csv.link_args) > 0:
            for linkfile in csv.link_args:
                write_output("Reading Link file %s" % linkfile)
                csv.read_links(linkfile)
        else:
            log.warn("No link files found")
            csv.warnings.append("No link files found")

        write_progress(5,csv.num_steps)
        if len(csv.group_args) > 0:
            for groupfile in csv.group_args:
                write_output("Reading Group file %s"% groupfile)
                csv.read_groups(groupfile)
        else:
            log.warn("No group files specified.")
            csv.warnings.append("No group files specified.")

        write_progress(6,csv.num_steps)
        if len(csv.groupmember_args) > 0:
            write_output("Reading Group Members")
            for groupmemberfile in csv.groupmember_args:
                csv.read_group_members(groupmemberfile)
        else:
            log.warn("No group member files specified.")
            csv.warnings.append("No group member files specified.")

        write_progress(7,csv.num_steps)
        write_output("Saving network")
        csv.commit()
        if csv.NetworkSummary.get('scenarios') is not None:
            scen_ids = [s['id'] for s in csv.NetworkSummary['scenarios']]

        write_progress(9,csv.num_steps)
        if len(csv.rule_args) > 0 and csv.rule_args[0] != "":
            write_output("Reading Rules")
            for s in csv.NetworkSummary.get('scenarios'):
                if s.name == csv.Scenario['name']:
                    scenario_id = s.id
                    break
            #Update all the nodes, links and groups with their newly
            #created IDs.
            rule_reader = RuleReader(csv.connection, scenario_id, csv.NetworkSummary, csv.rule_args)

            rule_reader.read_rules()

        network_id = csv.NetworkSummary['id']

        write_progress(9,csv.num_steps)
        write_output("Saving types")
        if args.template is not None:
            try:
                warnings = csv.set_resource_types(args.template)
                csv.warnings.extend(warnings)
            except Exception, e:
                raise HydraPluginError("An error occurred setting the types from the template. "
                                       "Error relates to \"%s\" "
                                       "Please check the template and resource types."%(e.message))
        write_progress(9,csv.num_steps)

	errors = []
    except HydraPluginError as e:
        errors = [e.message]
        log.exception(e)
    except Exception, e:
        log.exception(e)
        errors = [e]

    xml_response = PluginLib.create_xml_response('ImportCSV',
                                                 network_id,
                                                 scen_ids,
                                                 errors,
                                                 csv.warnings,
                                                 csv.message,
                                                 csv.files)

    print xml_response
