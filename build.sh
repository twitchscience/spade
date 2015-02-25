#!/bin/bash --
set -euo pipefail

# ossareh(20150109): Perhaps use something like:
# http://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash
PROJECT=$1
BRANCH=$2
SOURCE_AMI=$3
SECURITY_GROUP=$4

# I hate boolean args, but I'm not sure how to handle this.
USE_PRIVATE_IP=${5:-"false"}

export GOARCH=amd64
export GOOS=linux

# godep restore
godep go vet ./...
godep go test -v ./...
godep go install -v ./...

packer                                          \
     build -machine-readable                    \
     -var "project=${PROJECT}"                  \
     -var "branch=${BRANCH}"                    \
     -var "source_ami=${SOURCE_AMI}"            \
     -var "security_group_id=${SECURITY_GROUP}" \
     -var "use_private_ip=${USE_PRIVATE_IP}"    \
     -var "binary_dir"=${GOPATH}/bin            \
     -var "scripts_dir"=build/scripts           \
     build/packer.json
