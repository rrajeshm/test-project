#!/bin/bash -e
# @Author: grarthur
# @Date:   2019-03-19 12:21:36
# @Last Modified by:   Grant Arthur
# @Last Modified time: 2019-05-08 11:39:06

function usage () {
  echo "$0 <test_type> <path_to_config>"
  exit 1
}

function error () {
  echo "ERROR: $1"
  usage
}

test $# -eq 0 && usage

BASE_DIR=$(dirname "$0")
DEFAULT="$BASE_DIR/conf/settings.yaml"
TEST_TYPE="$1"
CONFIG="$2"

test "$CONFIG" || CONFIG="$DEFAULT"
test -e $CONFIG || error 'config file does not exists'

OC_USER=$(cat $CONFIG | python -c 'import yaml,sys; map=yaml.load(sys.stdin); print map["openshift"]["username"]')
OC_PASS=$(cat $CONFIG | python -c 'import yaml,sys; map=yaml.load(sys.stdin); print map["openshift"]["password"]')
OC_ADDR=$(cat $CONFIG | python -c 'import yaml,sys; map=yaml.load(sys.stdin); print map["openshift"]["address"]')

unset http_proxy
unset https_proxy
unset HTTPS_PROXY
unset HTTP_PROXY

mkdir -p "$BASE_DIR/conf/vmr/"

if [[ $CONFIG != $DEFAULT ]]; then
  cp $CONFIG "$DEFAULT"
fi

oc login -u $OC_USER -p $OC_PASS \
         --insecure-skip-tls-verify=true \
         --config="$BASE_DIR/conf/vmr/kubeconfig" \
         $OC_ADDR:8443

$BASE_DIR/dp_automation_main.py -t $TEST_TYPE --dist each