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
from HydraLib.HydraException import HydraPluginError
from csv_util import get_file_data, check_header
import logging
log = logging.getLogger(__name__)
import os
class RuleReader(object):
    
    ignorelines = ['', '\n', '\r']

    def __init__(self, connection, scenario_id, network, rule_files):
        self.connection = connection
        self.scenario_id = scenario_id
        self.Rules      = {}
        self.get_existing_rules()
        self.rule_files = rule_files


        self.Groups = {}
        self.Links  = {}
        self.Nodes  = {}
        for n in network.get('nodes'):
            self.Nodes[n.name] = n.id
        for l in network.get('links'):
            self.Links[l.name] = l.id
        for g in network.get('resourcegroups'):
            self.Groups[g.name] = g.id


    def get_existing_rules(self):
        rules = self.connection.call('get_rules', {'scenario_id':self.scenario_id})
        for r in rules:
            self.Rules[r.id] = r

    def read_rules(self):
        """
            Read all the rule files, one by one.
        """

        for file in self.rule_files:
            self.read_rule_file(file)

    def read_rule_file(self, file):
        """
            Read rules from a rule file. THe rule file looks like:
            Name,  Type, Resource, Text     , Description
            rule1, Node, Node1   , some text, Desctiption of some text
            ...
        """


        rule_data = get_file_data(file)

        keys  = rule_data[0].split(',')
        check_header(file, keys)

        data = rule_data[1:]

        #Indicates what the mandatory columns are and where
        #we expect to see them.
        field_idx = {'name': 0,
                     'type': 1,
                     'resource': 2,
                     'text':3,
                     'description':4,
                     }


        for line_num, line in enumerate(data):

            #skip any empty lines
            if line.strip() in self.ignorelines:
                continue
            try: 
                rule = self.read_rule_line(line, field_idx)
            except Exception, e:
                log.exception(e)
                raise HydraPluginError("An error has occurred in file %s at line %s: %s"%(os.path.split(file)[-1], line_num+3, e))
           
            self.Rules[rule['name']] = rule

        rules = self.connection.call("add_rules", {'scenario_id':self.scenario_id, 'rule_list':self.Rules.values()})

        return rules

    def read_rule_line(self, line, field_idx):
        """
            Read a single line from the rules file and return a rule object.
        """

        rule_data = line.split(',')
        rule_name = rule_data[field_idx['name']].strip()

        #Check if the rule already exists.
        if rule_name in self.Rules:
            rule = self.Rules[rule_name]
            rule_id = rule.id
            log.debug('rule %s exists.' % rule_name)
        else:
            ref_key = rule_data[field_idx['type']].strip().upper()
            ref_name = rule_data[field_idx['resource']].strip()
            rule_id=None
            try:
                if ref_key == 'NODE':
                    ref_id = self.Nodes[ref_name]
                elif ref_key == 'LINK':
                    ref_id = self.Links[ref_name]
                elif ref_key == 'GROUP':
                    ref_id = self.Groups[ref_name]
                else:
                    log.critical("Unknown reference type %s. Carrying on"%ref_key)
            except KeyError:
                raise HydraPluginError("Rule error: Unknown %s named %s. Please check the name is correct."%(ref_key.lower(), ref_name))

        rule = dict(
            id          = rule_id,
            name        = rule_name,
            description = rule_data[field_idx['description']].strip(),
            text        = rule_data[field_idx['text']].strip(),
            ref_key     = ref_key, 
            ref_id      = ref_id 
        )

        return rule

