# PAVICS-ncops
NetCDF Operations for the PAVICS project

Installation:

    docker build -t pavicsncops .

Configuration:

Provide the GEOSERVER host, as well as the exposed WPS host for output files
in the ncops.cfg file.

Running the application:

    docker run --name my_pavicsncops -d -p 8009:80 pavicsncops

The available processes can be obtained at:

    http://localhost:8009/pywps?service=WPS&request=GetCapabilities&version=1.0.0

The pywps config file (pywps.cfg) is available. However, the outputurl
and outputpath values should not be modified as they are currently
hardcoded in other places.
