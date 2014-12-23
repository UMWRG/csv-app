#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

- A link references to start and end node by name. This also implies that the
  node names defined in the file need to be unique.

- The description should be in the last column. This will lead to more readable
  files, since the description is usually free form text.

- If you specify more than one file containing nodes, common attributes (i.e.
  with the same name) will be considered the same. This results in a unique
  attribute set for nodes. The same applies for links.

- An attribute to a node or link is only added if there is a value for that
  specific attribute in the line specifying a node (or link). If an attribute
  should be added as a variable, you need to enter ``NULL``.

- If you use a tmplate during import, missing attributes will be added to each
  node or link according to its type.

TODO
----

- Implement updating of existing scenario.

Building a windows executable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 - Use pyinstaller (pip install pyisntaller) to build a windows executable.
 - cd to the $PATH_TO_HYDRA/HydraPlugins/CSVPlugin/trunk
 - pyinstaller -F ImportCSV.py
 or if you want more compression (a smaller exe), install upx
 - pyinstaller -F --upx-dir=/path/to/upx/dir ExportCSV.py
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
from HydraLib.PluginLib import JsonConnection, write_progress, write_output, validate_plugin_xml
from HydraLib import config, util, dateutil
from HydraLib.units import validate_resource_attributes

from HydraLib.HydraException import HydraPluginError,HydraError

from suds import WebFault
from numpy import array, reshape

from lxml import etree
import traceback

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
        self.scenario_names = []
        self.Nodes    = dict()
        self.Links    = dict()
        self.Groups   = dict()
        self.Attributes = dict()

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

        self.warnings = []
        self.message = ''
        self.files = []

        self.num_steps = 6

        self.ignorelines = ['', '\n', '\r']

    def validate_value(self, value, restriction_dict):
        if restriction_dict is None or restriction_dict == {}:
            return

        try:
            util.validate_value(restriction_dict, value)
        except HydraError, e:
            raise HydraPluginError(e.message)

    def get_file_data(self, file):
        """
            Taking a csv file as an argument,
            return an array where each element is a line in the csv.
        """
        file_data=None
        if file == None:
            log.warn("No file specified")
            return None
        self.basepath = os.path.dirname(os.path.realpath(file))
        with open(file, mode='r') as csv_file:
            file_data = csv_file.read().split('\n')
            if len(file_data) == 0:
                log.warn("File contains no data")
        # Ignore comments

        bad_lines = []
        for i, line in enumerate(file_data):
            try:
                line = ''.join([x if ord(x) < 128 else ' ' for x in line])
                line.decode('ascii')
            except UnicodeDecodeError, e:
                #If there are unknown characters in this line, save the line
                #and the column in the line where the bad character has occurred.
                bad_lines.append((i+1, e.start))

            if len(line.strip()) > 0 and line.strip()[0] == '#':
                file_data.pop(i)

        #Complain about the lines that the bad characters are on.
        if len(bad_lines) > 0:
            lines = [a[0] for a in bad_lines]
            raise HydraPluginError("Lines %s, in %s contain non ascii characters"%(lines, file))

        return file_data

    def check_header(self, file, header):
        """
            Check for common mistakes in headers:
            Duplicate columns
            Empty columns
        """
        if type(header) == str:
            header = header.split(',')

        for i, h in enumerate(header):
            if h.strip() == '':
                raise HydraPluginError("Malformed Header in %s: Column(s) %s is empty"%(file, i))

        individual_headings = []
        dupe_headings       = []
        for k in header:
            if k not in individual_headings:
                individual_headings.append(k)
            else:
                dupe_headings.append(k)
        if len(dupe_headings) > 0:
            raise HydraPluginError("Malformed Header: Duplicate columns: %s",
                                   dupe_headings)


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
                except WebFault:
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
                except WebFault:
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
            net_data = self.get_file_data(file)

            try:
                file_parts = file.split(".")
                file_base = file_parts[0]
                file_ext = file_parts[1]
                new_filename = "%s_metadata.%s"%(file_base, file_ext)
                metadata = self.read_metadata(new_filename)
            except IOError:
                log.info("No metadata found for node file %s",file)
                metadata = {}

            keys = net_data[0].split(',')
            self.check_header(file, keys)
            units = net_data[1].split(',')
            # A network file should only have one line of data
            data = net_data[2].split(',')

            for i, unit in enumerate(units):
                units[i] = unit.strip()

            # We assume a standard order of the network information (Name,
            # Description, attributes,...).
            field_idx = {'id': 0,
                         'name': 1,
                         'description': 2,
                         'projection':3,
                         'type': None,
                         }
            # If the file does not follow the standard, we can at least try to
            # guess what is stored where.
            for i, key in enumerate(keys):

                if key.lower().strip() in field_idx.keys():
                    field_idx[key.lower().strip()] = i

            if field_idx['type'] is not None:
                self.networktype = data[field_idx['type']].strip()

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
                except WebFault:
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
                self.Network = self.add_data(self.Network, attrs, data, metadata)

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
        metadata = self.get_file_data(filename)
        keys = metadata[0].split(',')
        self.check_header(filename, keys)
        data = metadata[1:-1]

        metadata_dict = {}
        for line_num, data_line in enumerate(data):
            try:
                split_data = data_line.split(',')
                metadata_dict[split_data[0].strip()] = self.get_metadata_as_dict(keys[1:], split_data[1:])
            except Exception, e:
                raise HydraPluginError("Malformed metadata on line %s in file %s"%(line_num+2, filename))
        return metadata_dict

    def get_metadata_as_dict(self, keys, metadata):
        """
            Turn a list of metadata values into a dictionary structure.
            @parameter keys to describe the attribte to which this metadata refers
            @parameter list of metadata. in the structure:
                ["(key;val) (key;val)", "(key;val) (key;val)",...]
            @returns dictionary in the format:
                {attr1 : {key:val, key:val}, attr2: {key:val, key:val}...}
        """
        metadata_dict = {}
        for i, attr in enumerate(keys):
            metadata_dict[attr.strip()] = {}
            if metadata[i].strip() != '':
                attr_metadata = metadata[i].split(")")
                for attr_meta in attr_metadata:
                    if attr_meta == '':
                        continue
                    attr_meta = attr_meta.replace('(', '')
                    keyval = attr_meta.split(';')
                    key = keyval[0].strip()
                    if key.lower() in ('name', 'dataset_name', 'dataset name'):
                        key = 'name'
                    val = keyval[1].strip()
                    metadata_dict[attr.strip()][key] = val

        return metadata_dict

    def read_nodes(self, file):
        node_data = self.get_file_data(file)

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
        self.check_header(file, keys)
        units = node_data[1].split(',')
        data = node_data[2:-1]

        for i, unit in enumerate(units):
            units[i] = unit.strip()

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
                raise HydraPluginError("An error has occurred in file %s at line %s: %s"%(file, line_num+3, e))

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
        link_data = self.get_file_data(file)

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
        self.check_header(file, keys)
        units = link_data[1].split(',')
        data = link_data[2:-1]

        for i, unit in enumerate(units):
            units[i] = unit.strip()

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
                raise HydraPluginError("An error has occurred in file %s at line %s: %s"%(file, line_num+3, e))

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
        group_data = self.get_file_data(file)

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
        self.check_header(file, keys)
        units = group_data[1].split(',')
        data  = group_data[2:-1]

        for i, unit in enumerate(units):
            units[i] = unit.strip()

        #Indicates what the mandatory columns are and where
        #we expect to see them.
        field_idx = {'name': 0,
                     'description': -1,
                     'type': None,
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
                raise HydraPluginError("An error has occurred in file %s at line %s: %s"%(file, line_num+3, e))
            
            self.Groups.update({group['name']: group})

    def read_group_line(self, line, attrs, field_idx, metadata, units):

        group_data = line.split(',')
        group_name = group_data[field_idx['name']].strip()


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
        member_data = self.get_file_data(file)

        keys  = member_data[0].split(',')
        self.check_header(file, keys)
        data  = member_data[2:-1]

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
                raise HydraPluginError("An error has occurred in file %s at line %s: %s"%(file, line_num+3, e))


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
                attribute['dimen'] = self.connection.call('get_dimension', {'unit1':unit.strip()})
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
            attributes = self.connection.call('add_attributes', {'attrs':attributes})
            self.add_attrs = False
            for attr in attributes:
                self.Attributes.update({attr['name']: attr})

        # Add data to each attribute
        for i in attrs.keys():
            attr = self.Attributes[attrs[i]]
            # Attribute might already exist for resource, use it if it does
            if attr['id'] in resource_attrs.keys():
                res_attr = resource_attrs[attr.id]
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

                else:
                    if metadata:
                        resource_metadata = metadata.get(resource['name'], {})
                        dataset_metadata = resource_metadata.get(attrs[i], {})
                    else:
                        dataset_metadata = {}

                    if units is not None:
                        if units[i] is not None and len(units[i].strip()) > 0:
                            dimension = attr['dimen']
                        else:
                            dimension = None

                        dataset = self.create_dataset(data[i],
                                                      res_attr,
                                                      units[i],
                                                      dimension,
                                                      resource['name'],
                                                      dataset_metadata,
                                                      restrictions.get(attr['name'], {}).get('restrictions', {})
                                                     )
                    else:
                        dataset = self.create_dataset(data[i],
                                                      res_attr,
                                                      None,
                                                      None,
                                                      resource['name'],
                                                      dataset_metadata,
                                                      restrictions.get(attr['name'], {}).get('restrictions', {})
                                                     )

                    if dataset is not None:
                        self.Scenario['resourcescenarios'].append(dataset)
        
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


    def create_dataset(self, value, resource_attr, unit, dimension, resource_name, metadata, restriction_dict):

        resourcescenario = dict()

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

        value = value.strip()
        if unit is not None:
            unit = unit.strip()
            if len(unit) == 0:
                unit = None
        arr_struct = None
        try:
            float(value)
            dataset['type'] = 'scalar'
            scal = self.create_scalar(value, restriction_dict)
            dataset['value'] = scal
        except ValueError:
            value = value.replace('\\', '/')
            try:
                filedata = []
                if self.expand_filenames:
                    full_file_path = os.path.join(self.basepath, value)
                    if self.file_dict.get(full_file_path) is None:
                        with open(full_file_path) as f:
                            filedata = []
                            for l in f:
                                l = l.strip().replace('\n', '').replace('\r', '')
                                filedata.append(l)
                            self.file_dict[full_file_path] = filedata
                    else:
                        filedata = self.file_dict[full_file_path]

                    value_header = filedata[0].lower().replace(' ', '')
                    if value_header.startswith('arraydescription,') or value_header.startswith(','):
                        arr_struct = filedata[0].strip()
                        arr_struct = arr_struct.split(',')
                        arr_struct = "|".join(arr_struct[3:])
                        filedata = filedata[1:]
                    elif value_header.startswith('timeseriesdescription') or value_header.startswith(','):
                        arr_struct = filedata[0].strip()
                        arr_struct = arr_struct.split(',')
                        arr_struct = "|".join(arr_struct[3:])
                        filedata = filedata[1:]

                    #The name of the resource is how to identify the data for it.
                    #Once this the correct line(s) has been identified, remove the
                    #name from the start of the line
                    data = []
                    for l in filedata:
                        l_resource_name = l.split(',',1)[0].strip()
                        if l_resource_name == resource_name:
                            data.append(l[l.find(',')+1:])

                    if len(filedata) == 0:
                        log.info('%s: No data found in file %s' %
                                     (resource_name, full_file_path))
                        self.warnings.append('%s: No data found in file %s' %
                                             (resource_name, full_file_path))
                        return None
                    else:
                        if self.is_timeseries(data):
                            ts_type, ts = self.create_timeseries(data, restriction_dict)
                            dataset['type'] = ts_type
                            dataset['value'] = ts
                        else:
                            dataset['type'] = 'array'
                            if len(filedata) > 0:
                                try:
                                    dataset['value'] = self.create_array(data[0], restriction_dict)
                                except Exception, e:
                                    raise HydraPluginError("There is a value error in %s. Please check value %s is correct."%(value, data[0]))
                            else:
                                dataset['value'] = None
                else:
                    raise IOError
            except IOError:
                dataset['type'] = 'descriptor'
                desc = self.create_descriptor(value, restriction_dict)
                dataset['value'] = desc

        dataset['unit'] = unit
        if unit is not None:
            dataset['dimension'] = dimension

        dataset['name'] = self.Scenario['name']

        resourcescenario['value'] = dataset

        m = []
        if arr_struct:
            m.append(dict(name='data_struct',value=arr_struct))
        if metadata:
            for k, v in metadata.items():
                if k == 'data_struct' and arr_struct:
                    continue
                m.append(dict(name=k,value=v))


        dataset['metadata'] = m

        return resourcescenario

    def create_scalar(self, value, restriction_dict={}):
        self.validate_value(value, restriction_dict)
        scalar = dict(
            param_value = str(value)
        )
        return scalar

    def create_descriptor(self, value, restriction_dict={}):
        self.validate_value(value, restriction_dict)
        descriptor = dict(
            desc_val = value
        )
        return descriptor

    def create_timeseries(self, data, restriction_dict={}):
        if len(data) == 0:
            return None

        date = data[0].split(',', 1)[0].strip()
        timeformat = dateutil.guess_timefmt(date)
        seasonal = False
        if 'XXXX' in timeformat:
            seasonal = True

        ts_values = []
        start_time = None
        freq       = None
        prev_time  = None
        eq_val     = []
        is_eq_spaced = False
        timedata = data
        for line in timedata:
            if line != '':
                dataset = line.split(',')
                tstime = datetime.strptime(dataset[0].strip(), timeformat)
                tstime = self.timezone.localize(tstime)

                ts_time = dateutil.date_to_string(tstime, seasonal=seasonal)

                value_length = len(dataset[2:])
                shape = dataset[1].strip()
                if shape != '':
                    array_shape = tuple([int(a) for a in
                                         shape.split(" ")])
                else:
                    array_shape = (value_length,)

                if array_shape == (1,):
                    ts_value = dataset[2].strip()
                else:
                    ts_val_1d = []
                    for i in range(value_length):
                        ts_val_1d.append(str(dataset[i + 2].strip()))

                    ts_arr = array(ts_val_1d)
                    ts_arr = reshape(ts_arr, array_shape)

                    ts_value = ts_arr.tolist()

                #Check for whether timeseries is equally spaced.
                if is_eq_spaced:
                    eq_val.append(ts_value)
                    if start_time is None:
                        start_time = tstime
                    else:
                        #Get the time diff as the second time minus the first
                        if freq is None:
                            freq = tstime - start_time
                        else:
                            #Keep checking on each timestamp whether the spaces between
                            #times is equal.
                            if (tstime - prev_time) != freq:
                                is_eq_spaced = False
                    prev_time = tstime

                ts_values.append({'ts_time': ts_time,
                                  'ts_value': ts_value
                                  })

        if is_eq_spaced:
            ts_type = 'eqtimeseries'
            timeseries = {'frequency': freq.total_seconds(),
                          'start_time':dateutil.date_to_string(start_time, seasonal=seasonal),
                          'arr_data':str(eq_val)}
            self.validate_value(timeseries, restriction_dict)
        else:
            ts_type = 'timeseries'
            timeseries = {'ts_values': ts_values}
            self.validate_value(timeseries, restriction_dict)

            #After validating the data, turn it into the format the server wants.
            for ts in timeseries['ts_values']:
                ts['ts_value'] = PluginLib.create_dict(ts['ts_value'])


        return ts_type, timeseries

    def create_array(self, data, restriction_dict={}):
        #Split the line into a list
        dataset = data.split(',')
        #First column is always the array dimensions
        arr_shape = dataset[0]
        #The actual data is everything after column 0
        dataset = [eval(d) for d in dataset[1:]]

        #If the dimensions are not set, we assume the array is 1D
        if arr_shape != '':
            array_shape = tuple([int(a) for a in arr_shape.strip().split(" ")])
        else:
            array_shape = (len(dataset),)

        #Reshape the array back to its correct dimensions
        arr = array(dataset)
        try:
            arr = reshape(arr, array_shape)
        except:
            raise HydraPluginError("You have an error with your array data."
                                   " Please ensure that the dimension is correct."
                                   " (array = %s, dimension = %s)" %(arr, array_shape))

        self.validate_value(arr.tolist(), restriction_dict)

        arr = dict(
            arr_data = PluginLib.create_dict(arr.tolist())
        )

        return arr

    def is_timeseries(self, data):
        date = data[0].split(',')[0].strip()
        timeformat = dateutil.guess_timefmt(date)
        if timeformat is None:
            return False
        else:
            return True

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

    def validate_template(self, template_file):

        log.info('Validating template file (%s).' % template_file)

        with open(template_file) as f:
            xml_template = f.read()

        template_xsd_path = os.path.expanduser(config.get('templates', 'template_xsd_path'))
        log.info("Template xsd: %s",template_xsd_path)
        xmlschema_doc = etree.parse(template_xsd_path)
        xmlschema = etree.XMLSchema(xmlschema_doc)
        xml_tree = etree.fromstring(xml_template)

        try:
            xmlschema.assertValid(xml_tree)
        except etree.DocumentInvalid as e:
            raise HydraPluginError('Template validation failed: ' + e.message)

        template_dict = {'name':xml_tree.find('template_name').text,
                         'resources' : {}
                        }

        for r in xml_tree.find('resources'):
            resource_dict = {}
            resource_name = r.find('name').text
            resource_type = r.find('type').text
            resource_dict['type'] = resource_type
            resource_dict['name'] = resource_name
            resource_dict['attributes'] = {}
            for attr in r.findall("attribute"):
                attr_dict = {}
                attr_name = attr.find('name').text
                attr_dict['name'] = attr_name
                if attr.find('dimension') is not None:
                    attr_dict['dimension'] = attr.find('dimension').text
                if attr.find('is_var') is not None:
                    attr_dict['is_var'] = attr.find('is_var').text
                if attr.find('data_type') is not None:
                    attr_dict['data_type'] = attr.find('data_type').text

                restction_xml = attr.find("restrictions")
                attr_dict['restrictions'] = util.get_restriction_as_dict(restction_xml)
                resource_dict['attributes'][attr_name] = attr_dict

            if template_dict['resources'].get(resource_type):
                template_dict['resources'][resource_type][resource_name] = resource_dict
            else:
                template_dict['resources'][resource_type] = {resource_name : resource_dict}

        self.Templates = template_dict

        log.info("Template OK")


def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a network saved in a set of CSV files into Hydra.

        Written by Philipp Meier <philipp@diemeiers.ch> and Stephen Knox <stephen.knox@manchester.ac.uk>
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
    parser.add_argument('-n', '--nodes', nargs='+',
                        help='''One or multiple files containing nodes and
                        attributes.''')
    parser.add_argument('-l', '--links', nargs='+',
                        help='''One or multiple files containing information
                        on links.''')
    parser.add_argument('-i', '--network_id',
                        help='''The ID of an existing network. If specified,
                        this network will be updated. If not, a new network
                        will be created.
                        on links.''')
    parser.add_argument('-g', '--groups', nargs='+',
                        help='''One or multiple files containing information
                        on groups and their attributes (but not members)''')
    parser.add_argument('-k', '--groupmembers', nargs='+',
                        help='''One or multiple files containing information
                        on the members of groups.
                        The groups (-g argument) file must be specified if this
                        argument is specified''')
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
    parser.add_argument('-u', '--server-url',
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

        validate_plugin_xml(os.path.join(__location__, 'plugin.xml'))

        if args.expand_filenames:
            csv.expand_filenames = True

        if args.timezone is not None:
            csv.timezone = pytz.timezone(args.timezone)

        if args.nodes is not None:
            # Validation

            if args.template is not None:
                try:
                    if args.template == '':
                        raise Exception("The template name is empty.")
                    csv.validate_template(args.template)
                except Exception, e:
                    raise HydraPluginError("An error has occurred with the template. (%s)"%(e))

            # Create project and network only when there is actual data to
            # import.
            csv.create_project(ID=args.project, network_id=args.network_id)
            csv.create_scenario(name=args.scenario)
            csv.create_network(file=args.network, network_id=args.network_id)

            write_progress(1,csv.num_steps)
            for nodefile in args.nodes:
                write_output("Reading Node file %s" % nodefile)
                csv.read_nodes(nodefile)

            write_progress(2,csv.num_steps)
            if args.links is not None:
                for linkfile in args.links:
                    write_output("Reading Link file %s" % linkfile)
                    csv.read_links(linkfile)

            write_progress(3,csv.num_steps)
            if args.groups is not None:
                for groupfile in args.groups:
                    write_output("Reading Group file %s"% groupfile)
                    csv.read_groups(groupfile)

            write_progress(4,csv.num_steps)
            if args.groupmembers is not None:
                write_output("Reading Group Members")
                if args.groups is None:
                    raise HydraPluginError("Cannot specify a group member "
                                           "file without a matching group file.")
                for groupmemberfile in args.groupmembers:
                    csv.read_group_members(groupmemberfile)

            write_progress(5,csv.num_steps)
            write_output("Saving network")
            csv.commit()
            if csv.NetworkSummary.get('scenarios') is not None:
                scen_ids = [s['id'] for s in csv.NetworkSummary['scenarios']]

            network_id = csv.NetworkSummary['id']

            write_output("Saving types")
            if args.template is not None:
                try:
                    warnings = csv.set_resource_types(args.template)
                    csv.warnings.extend(warnings)
                except Exception, e:
                    raise HydraPluginError("An error occurred setting the types from the template. "
                                           "Error relates to \"%s\" "
                                           "Please check the template and resource types."%(e.message))
            write_progress(6,csv.num_steps)

        else:
            log.info('No nodes found. Nothing imported.')

	errors = []
    except HydraPluginError as e:
        errors = [e.message]
    except Exception, e:
        log.critical(e)
        traceback.print_exc(file=sys.stdout)
        errors = [e]

    xml_response = PluginLib.create_xml_response('ImportCSV',
                                                 network_id,
                                                 scen_ids,
                                                 errors,
                                                 csv.warnings,
                                                 csv.message,
                                                 csv.files)

    print xml_response
