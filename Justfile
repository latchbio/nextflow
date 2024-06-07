bucket := "latch-public"
nextflow_dir := "nextflow-v2"
version := `echo $(cat LATCH_VERSION) | tr -d '\n'`
path := "s3://" + bucket + "/" + nextflow_dir + "/" + version

build:
  make clean
  make compile
  make install

upload:
  #!/usr/bin/env bash

  if aws s3 ls {{path}} > /dev/null;
  then
    echo 'Nextflow version already exists'
    exit 1
  fi

  echo Uploading to {{path}}

  aws s3 rm --recursive {{path}}/.nextflow
  aws s3 cp --recursive --quiet $HOME/.nextflow {{path}}/.nextflow
  aws s3 cp --quiet nextflow {{path}}/nextflow

do-the-thing: build upload
