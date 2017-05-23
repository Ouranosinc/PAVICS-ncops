import os
import time
import logging
from logging.config import dictConfig

from pywps import Process, get_format, configuration
from pywps import LiteralInput, ComplexOutput

from pavics import ncgeo
from pavics import geoserver

# Example usage:
# http://localhost:8009/pywps?service=WPS&request=execute&version=1.0.0&\
# identifier=spatial_weighted_average&storeExecuteResponse=true&status=true&\
# DataInputs=\
# resource=http://x.x.x.x:8083/thredds/dodsC/birdhouse/ncep/cfsr/pr/\
# pr_1hr_cfsr_reanalysis_197901.nc;\
# typename=usa:states;featureids=states.4

env_geoserver_host = os.environ['GEOSERVER_HOST']
wfs_server = ("http://{0}/geoserver/ows?service=WFS&version=1.0.0"
              "&request=GetFeature&typeName=")
wfs_server = wfs_server.format(env_geoserver_host)


netcdf_output_path = configuration.get_config_value('server', 'outputpath')
json_format = get_format('JSON')
netcdf_format = get_format('NETCDF')


class SpatialWeightedAverage(Process):
    def __init__(self):
        # From pywps4 code : time_format = '%Y-%m-%dT%H:%M:%S%z'
        # Is that a bug? %z should be %Z
        # Using 'string' data_type until this is corrected.
        inputs = [LiteralInput('resource',
                               'Resource',
                               data_type='string',
                               min_occurs=0,
                               max_occurs=1),
                  LiteralInput('typename',
                               'TypeName',
                               data_type='string',
                               min_occurs=0,
                               max_occurs=1),
                  LiteralInput('featureids',
                               'Feature Ids',
                               data_type='string',
                               min_occurs=0,
                               max_occurs=1)]

        outputs = [ComplexOutput('output_netcdf',
                                 'Output NetCDF',
                                 supported_formats=[netcdf_format])]
        outputs[0].as_reference = True

        super(SpatialWeightedAverage, self).__init__(
            self._handler,
            identifier='spatial_weighted_average',
            title='Spatial Weighted Average',
            abstract='Pending.',
            version='0.1',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True)

    def _handler(self, request, response):
        time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        log_file_name = "log_file_%s_" % (time_str,)
        log_file = os.path.join(netcdf_output_path, log_file_name)
        logger = logging.getLogger(__name__)
        lf = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        dictConfig({'version': 1,
                    'disable_existing_loggers': False,
                    'formatters': {'standard': {'format': lf}},
                    'handlers': {'logfile': {'level': 'DEBUG',
                                             'class': 'logging.FileHandler',
                                             'filename': log_file,
                                             'formatter': 'standard'}},
                    'loggers': {'': {'handlers': ['logfile'],
                                     'level': 'DEBUG',
                                     'propagate': True}}})

        resource = request.inputs['resource'][0].data
        typename = request.inputs['typename'][0].data
        featureids = request.inputs['featureids'][0].data

        featureids = featureids.split(',')
        geometry = geoserver.shapely_from_geoserver(
            wfs_server, typename, feature_ids=featureids)
        suffix = '_spatial_weighted_average.nc'
        out_file = os.path.basename(resource)[:-3] + suffix
        out_file = os.path.join(netcdf_output_path, 'pavics-ncops', out_file)
        logger.error(out_file)
        ncgeo.spatial_weighted_average(resource, out_file, geometry)

        response.outputs['output_netcdf'].file = out_file
        response.outputs['output_netcdf'].output_format = netcdf_format
        return response
