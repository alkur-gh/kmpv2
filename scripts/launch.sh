#!/bin/bash

curl http://localhost:8080/control/start -d '{"id": "node-0", "serfAddr": "127.0.0.1:8000", "bootstrap": true}'
sleep 5
curl http://localhost:8180/control/start -d '{"id": "node-1", "joinAddrs": ["127.0.0.1:8000"]}'
curl http://localhost:8280/control/start -d '{"id": "node-2", "joinAddrs": ["127.0.0.1:8000"]}'
