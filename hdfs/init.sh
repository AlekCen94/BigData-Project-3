#!/bin/bash

sh bootstrap.sh -bash
hadoop dfsadmin -safemode wait
hadoop dfs -mkdir /data
hadoop dfs -put /opt/hadoopspark/ /data
hadoop dfs -mkdir -p /user/jovyan/model
hadoop dfs -chown -R jovyan /user/jovyan
hadoop dfs -chmod -R 777 /user/jovyan


