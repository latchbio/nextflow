bucket := "latch-public"
subdir := "nextflow-v2"
version := `echo $(cat LATCH_VERSION) | tr -d '\n'`
nextflow_dir := "s3://" + bucket + "/" + subdir
path := nextflow_dir + "/" + version

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

upload-archive:
  #!/usr/bin/env bash

  CUR_DIR=$(pwd)

  cd $HOME
  tar -cvzf $CUR_DIR/nextflow.tar.gz .nextflow
  cd $CUR_DIR
  aws s3 cp --quiet nextflow.tar.gz {{path}}/nextflow.tar.gz


publish:
  aws s3 cp LATCH_VERSION {{nextflow_dir}}/LATEST

do-the-thing: build upload upload-archive publish
