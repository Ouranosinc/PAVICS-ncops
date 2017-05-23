import os
import time
import json

from pywps import Process, get_format, configuration
from pywps import LiteralInput, ComplexOutput

from pavics import ncgeo
from pavics import geoserver

# Example usage:
#
# localhost/pywps?service=WPS&request=execute&version=1.0.0&\
# identifier=getpoint&DataInputs=\
# opendap_url=http://132.217.140.45:8083/thredds/dodsC/birdhouse/ouranos/\
# subdaily/aev/shum/aev_shum_1962.nc;\
# opendap_url=http://132.217.140.45:8083/thredds/dodsC/birdhouse/ouranos/\
# subdaily/aev/shum/aev_shum_1963.nc;variable=SHUM;ordered_indice=0;\
# ordered_indice=0,ordered_indice=70,ordered_indice=30

json_output_path = configuration.get_config_value('server', 'outputpath')
json_format = get_format('JSON')
netcdf_format = {"mimeType":"application/x-netcdf"}


class SpatialWeightedAverage(Process):
    def __init__(self):
        # From pywps4 code : time_format = '%Y-%m-%dT%H:%M:%S%z'
        # Is that a bug? %z should be %Z
        # Using 'string' data_type until this is corrected.
        inputs = [LiteralInput('resource',
                               'Resource',
                               data_type='string',
                               min_occurs=0
                               max_occurs=1),
                  LiteralInput('typename',
                               'TypeName',
                               data_type='string',
                               min_occurs=0,
                               max_occurs=1),
                  LiteralInput('featureids',
                               'Feature Ids',
                               data_type='boolean',
                               default=False,
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

    def _value_or_default(self, request, input_name):
        if input_name in request.inputs:
            return [request.inputs[input_name][n].data
                    for n in range(len(request.inputs[input_name]))]
            return request.inputs[input_name][0].data
        else:
            # workaround for poor handling of default values
            return [x.default
                    for x in self.inputs if x.identifier == input_name]

    def _dict_from_sep(self,xs,sep=':',convert=None):
        d = {}
        for x in xs:
            decode_x = x.split(sep)
            if convert is None:
                d[decode_x[0]] = sep.join(decode_x[1:])
            else:
                d[decode_x[0]] = convert(sep.join(decode_x[1:]))
        return d

    def _handler(self, request, response):
        f = self._value_or_default
        g = self._dict_from_sep
        resource = f(request, 'resource')
        typename = f(request, 'typename')
        featureids = f(request, 'featureids')

        featureids = featureids.split(',')
        geometry = geoserver.shapely_from_geoserver(
            wfs_server, typename, feature_ids=featureids)
        suffix = '_spatial_weighted_average.nc'
        out_file = os.path.basename(resource)[:-3]+suffix
        out_file = os.path.join(netcdf_output_path, 'pavics-ncops', out_file)
        ncgeo.spatial_weighted_average(resource, out_file, geometry)

        # Here we construct a unique filename
        time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        output_file_name = "point_result_%s_.json" % (time_str,)
        output_file = os.path.join(json_output_path, output_file_name)
        f1 = open(output_file, 'w')
        f1.write(json.dumps(point_result))
        f1.close()
        response.outputs['output_netcdf'].file = out_file
        response.outputs['output_netcdf'].output_format = netcdf_format
        return response
