import os
import time
import logging
from logging.config import dictConfig
import http.client
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from pywps import Process, get_format, configuration
from pywps import LiteralInput, ComplexOutput

import netCDF4
from pavics import ncgeo
from pavics import geoserver

# Example usage:
# http://localhost:8009/pywps?service=WPS&request=execute&version=1.0.0&\
# identifier=subset_polygon&storeExecuteResponse=true&status=true&\
# DataInputs=\
# resource=http://x.x.x.x:8083/thredds/dodsC/birdhouse/ncep/cfsr/pr/\
# pr_1hr_cfsr_reanalysis_197901.nc;\
# typename=usa:states;featureids=states.4

env_geoserver_host = os.environ['GEOSERVER_HOST']
wfs_server = ("http://{0}/geoserver/ows?service=WFS&version=1.0.0"
              "&request=GetFeature&typeName=")
wfs_server = wfs_server.format(env_geoserver_host)


netcdf_output_path = configuration.get_config_value('server', 'outputpath')
# In the context of the wps server running from docker, where the output
# path comes from a docker volume, we may need to recreate this directory:
if not os.path.isdir(netcdf_output_path):
    os.makedirs(netcdf_output_path)
json_format = get_format('JSON')
netcdf_format = get_format('NETCDF')


# This should really be somewhere else...
def conn_port_fix(conn_fn, netloc):
    decode_netloc = netloc.split(':')
    if len(decode_netloc) == 1:
        return conn_fn(netloc)
    else:
        return conn_fn(decode_netloc[0], decode_netloc[-1])


# This should really be somewhere else...
def url_result(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme == 'http':
        conn = conn_port_fix(http.client.HTTPConnection, parsed_url.netloc)
    elif parsed_url.scheme == 'https':
        conn = conn_port_fix(http.client.HTTPSConnection, parsed_url.netloc)
    reconstructed_url = parsed_url.path
    if parsed_url.params:
        reconstructed_url = reconstructed_url + ';' + parsed_url.params
    if parsed_url.query:
        reconstructed_url = reconstructed_url + '?' + parsed_url.query
    if parsed_url.fragment:
        reconstructed_url = reconstructed_url + '#' + parsed_url.fragment
    conn.request("GET", reconstructed_url)
    r1 = conn.getresponse()
    url_response = r1.read()
    conn.close()
    return url_response


# This should really be somewhere else...
def http_download(url, target):
    f1 = open(target, 'wb')
    f1.write(url_result(url))
    f1.close()


class SubsetPolygon(Process):
    """Return the subset of the netCDF file located inside a polygon.""""
    def __init__(self):
        inputs = [LiteralInput('resource',
                               'Resource',
                               'URL to netCDF file.',   
                               data_type='string',
                               min_occurs=0,
                               max_occurs=1),
                  LiteralInput('typename',
                               'TypeName',
                               'The feature collection.',
                               data_type='string',
                               min_occurs=0,
                               max_occurs=1),
                  LiteralInput('featureids',
                               'Feature Ids',
                               'The feature IDs', 
                               data_type='string',
                               min_occurs=0,
                               max_occurs=1)]

        outputs = [ComplexOutput('output_netcdf',
                                 'Output NetCDF',
                                 supported_formats=[netcdf_format])]
        outputs[0].as_reference = True

        super(SubsetPolygon, self).__init__(
            self._handler,
            identifier='subset_polygon',
            title='Subset polygon',
            abstract='Pending.',
            version='0.1',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True)

    def _handler(self, request, response):
        time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        log_file_name = "log_file_%s_" % (time_str,)
        log_file = os.path.join('/opt', log_file_name)
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
        # If we get a link to a file, we need to download it since
        # the ncgeo functions do not support downloading (yet?). There's
        # also the case where an intermediate output from another process
        # might already be on the thredds server through a docker volume
        # in which case it would be more efficient to use its opendap url,
        # but this is not returned by the process itself...
        # Presently, let's just download files when necessary...
        try:
            nc = netCDF4.Dataset(resource, 'r')
            nc.close()
        except RuntimeError as e:
            if e.message == 'NetCDF: Authorization failure':
                raise NotImplementedError('Authentication required.')
            else:
                # This is possibly a remote file, try to download it
                nc_tmp = os.path.join('/tmp', os.path.basename(resource))
                #http_download(resource, nc_tmp)
                os.system("curl {0} -o {1}".format(resource, nc_tmp))
                try:
                    nc = netCDF4.Dataset(nc_tmp, 'r')
                    nc.close()
                except:
                    raise IOError('Failed to download and open file.')
                resource = nc_tmp

        typename = request.inputs['typename'][0].data
        featureids = request.inputs['featureids'][0].data

        featureids = featureids.split(',')
        geometry = geoserver.shapely_from_geoserver(
            wfs_server, typename, feature_ids=featureids)
        suffix = '_subset_polygon_.nc'
        out_file = os.path.basename(resource)[:-3] + suffix
        out_file = os.path.join('/opt', out_file)
        ncgeo.subset_polygon(resource, out_file, geometry)

        response.outputs['output_netcdf'].file = out_file
        response.outputs['output_netcdf'].output_format = netcdf_format
        return response
