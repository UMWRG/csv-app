from HydraLib.HydraException import HydraPluginError, HydraError
from HydraLib import util
import os
import logging
log = logging.getLogger(__name__)


def get_file_data(file):
    """
        Taking a csv file as an argument,
        return an array where each element is a line in the csv.
    """
    file_data=None
    if file == None:
        log.warn("No file specified")
        return None
   
    file = os.path.realpath(file)
    
    log.info("Reading file data from: %s", file)

    with open(file, mode='r') as csv_file:
        file_data = csv_file.read().split('\n')
        if len(file_data) == 0:
            log.warn("File contains no data")

    new_file_data = []
    bad_lines = []
    for i, line in enumerate(file_data):
        line = line.strip()
        
        # Ignore comments            
        if len(line) == 0 or line[0] == '#':
            continue
        try:
            line = ''.join([x if ord(x) < 128 else ' ' for x in line])
            line.decode('utf-8')
            new_file_data.append(line)
        except UnicodeDecodeError, e:
            #If there are unknown characters in this line, save the line
            #and the column in the line where the bad character has occurred.
            bad_lines.append((i+1, e.start))

    #Complain about the lines that the bad characters are on.
    if len(bad_lines) > 0:
        lines = [a[0] for a in bad_lines]
        raise HydraPluginError("Lines %s, in %s contain non ascii characters"%(lines, file))

    return new_file_data

def check_header(file, header):
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
        raise HydraPluginError("Malformed Header in file %s: Duplicate columns: %s"%
                               (file , dupe_headings))

def validate_value(value, restriction_dict):
    if restriction_dict is None or restriction_dict == {}:
        return

    try:
        util.validate_value(restriction_dict, value)
    except HydraError, e:
        log.exception(e)
        raise HydraPluginError(e.message)

def parse_unit(unit):
    try:
        float(unit[0])
        factor, unit = unit.split(' ', 1)
        return unit, float(factor)
    except ValueError:
        return unit, 1.0