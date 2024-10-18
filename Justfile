bucket := "latch-public"
subdir := "nextflow-v2"
version := `echo $(cat LATCH_VERSION) | tr -d '\n'`
nextflow_dir := "s3://" + bucket + "/" + subdir
path := nextflow_dir + "/" + version

build-sync:
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags '-extldflags "-static"' -o custom_fsync.bin custom_fsync/sync.go
  chmod +x custom_fsync

build:
  #!/usr/bin/env bash

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

  CUR_DIR=$(pwd)

  cd $HOME
  tar -cvzf $CUR_DIR/nextflow.tar.gz .nextflow
  cd $CUR_DIR
  aws s3 cp --quiet nextflow.tar.gz {{path}}/nextflow.tar.gz

  aws s3 cp --quiet nextflow {{path}}/nextflow

upload-sync:
  aws s3 rm --quiet {{nextflow_dir}}/custom_fsync
  aws s3 cp --quiet custom_fsync.bin {{nextflow_dir}}/custom_fsync

push-sync: build-sync upload-sync

publish:
  #!/usr/bin/env bash

  aws s3 cp LATCH_VERSION {{nextflow_dir}}/LATEST

do-the-thing: build upload publish
