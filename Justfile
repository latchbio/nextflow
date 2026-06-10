bucket := "latch-public"
subdir := "nextflow-v2"
version := `echo $(cat LATCH_VERSION) | tr -d '\n'`
nextflow_dir := "s3://" + bucket + "/" + subdir

nextflow_version := `echo $(cat VERSION) | tr -d '\n'`

path := nextflow_dir + "/" + nextflow_version + "/" + version

build-sync:
  cargo build --release --manifest-path custom_fsync/Cargo.toml --target x86_64-unknown-linux-musl
  cp custom_fsync/target/x86_64-unknown-linux-musl/release/custom_fsync custom_fsync.bin
  chmod +x custom_fsync.bin

build:
  #!/usr/bin/env bash

  make clean
  make compile
  make install

  rm -rf ~/.nextflow/plugins/nf-k8s-1.0.1
  mkdir -p ~/.nextflow/plugins/nf-k8s-1.0.1
  cp -r plugins/nf-k8s/build/classes/groovy/main ~/.nextflow/plugins/nf-k8s-1.0.1/classes
  if [[ -d plugins/nf-k8s/build/classes/java/main ]]; then
    cp -r plugins/nf-k8s/build/classes/java/main/* ~/.nextflow/plugins/nf-k8s-1.0.1/classes
  fi
  if [[ -d plugins/nf-k8s/build/resources/main ]]; then
    cp -r plugins/nf-k8s/build/resources/main/* ~/.nextflow/plugins/nf-k8s-1.0.1/classes
  fi

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
