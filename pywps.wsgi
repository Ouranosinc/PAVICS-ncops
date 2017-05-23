#!/usr/bin/env python
import sys
from pywps.app import Service

sys.path.append('/var/www/html/wps')

# Must first import the process, then add it to the application.
from wps_subset_polygon import SubsetPolygon
from wps_spatial_weighted_average import SpatialWeightedAverage

application = Service(processes=[SubsetPolygon(),
                                 SpatialWeightedAverage()])
