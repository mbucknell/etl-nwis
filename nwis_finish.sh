#!/bin/bash

echo "Hit the cache clear service"
curl http://www.waterqualitydata.us/ogcservices/rest/clearcache/wqp_sites > /dev/null 2>&1
