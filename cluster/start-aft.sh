#!/bin/bash

#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

IP=`ifconfig eth0 | grep 'inet addr:' | grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1 }'`

# A helper function that takes a space separated list and generates a string
# that parses as a YAML list.
gen_yml_list() {
  IFS=' ' read -r -a ARR <<< $1
  RESULT=""

  for IP in "${ARR[@]}"; do
    RESULT=$"$RESULT        - $IP\n"
  done

  echo -e "$RESULT"
}

# Fetch the most recent version of the code.
cd $AFT_HOME
git fetch -p origin
git checkout -b brnch origin/$REPO_BRANCH
cd proto/aft
protoc -I . aft.proto --go_out=plugins=grpc:.

# Build the most recent version of the code.
go build

# Wait for the aft-config file to be passed in.
while [[ ! -f aft-config.yml ]]; do
  X=1 # Empty command to pass.
done

# Generate the YML config file.
echo "ipAddress: $IP" >> aft-config.yml
LST=$(gen_yml_list "$REPLICA_IPS")
echo "replicaList:" >> aft-config.yml
echo "$LST" >> aft-config.yml

# Start the process.
./aft
