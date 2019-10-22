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

IP=`ifconfig eth0 | grep 'inet' | grep -v inet6 | awk '{print $2}'`

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

# Create the AWS access key infrastructure.
mkdir -p ~/.aws
echo -e "[default]\nregion = us-east-1" > ~/.aws/config
echo -e "[default]\naws_access_key_id = $AWS_ACCESS_KEY_ID\naws_secret_access_key = $AWS_SECRET_ACCESS_KEY" > ~/.aws/credentials

# Fetch the most recent version of the code.
cd $AFT_HOME
git fetch -p origin
git checkout -b brnch origin/$REPO_BRANCH
cd proto/aft
protoc -I . aft.proto --go_out=plugins=grpc:.
cd $AFT_HOME

# Build the most recent version of the code.
go build

# Wait for the aft-config file to be passed in.
while [[ ! -f $AFT_HOME/conf/aft-config.yml ]]; do
  X=1 # Empty command to pass.
done

# Generate the YML config file.
echo "ipAddress: $IP" >> conf/aft-config.yml
LST=$(gen_yml_list "$REPLICA_IPS")
echo "replicaList:" >> conf/aft-config.yml
echo "$LST" >> conf/aft-config.yml

# Start the process.
if [[ "$ROLE" = "aft" ]]; do
  ./aft
elif [[ "$ROLE" = "bench" ]]
  cd $AFT_HOME/benchmark
  go build
  python3 benchmark_server.py
done
