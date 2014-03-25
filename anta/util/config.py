#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import logging.config
import os

from anta.util import jsonbson


def find_config_path():
    # why not os.path.abspath(sys.modules['__main__'].__file__)
    locations = [
        os.path.abspath(os.getcwd()),
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".."),
        os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."),
        os.path.abspath(os.path.join(os.getcwd(), 'anta')),
        os.path.abspath(os.path.join(os.getcwd(), os.pardir)),
        os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir)),
        os.path.expanduser("~"),
        "/etc/anta",
        os.environ.get("ANTA_CONF")
    ]
    for location in locations:
        print("location: {}".format(location))
        config_location = os.path.join(location, "conf", "config.json")
        if os.path.exists(config_location):
            return os.path.join(location, "conf")


def load_config_json():
    #print("config.load_config_json")
    try:
        config_location = os.path.join(config_path, "config.json")
        with open(config_location, 'r') as config_file:
            print("config_location: {}".format(config_location))
            return jsonbson.load_json_file(config_file)
    except IOError as e:
        print "ERROR: Can' open config.json file", e
    except ValueError as e:
        print "ERROR: Config file is not valid JSON", e
        return False



config_path = find_config_path()
config = load_config_json()
logging.config.dictConfig(config["logging"])
