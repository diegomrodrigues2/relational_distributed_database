#!/bin/bash
python -m grpc_tools.protoc -I database/replication \
    --python_out=. --grpc_python_out=. \
    database/replication/replica/router.proto
