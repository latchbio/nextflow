build:
  make clean
  make compile
  make install

  make assemble
  rm -rf ${HOME}/.nextflow/plugins/nf-amazon-2.2.1
  mkdir -p ${HOME}/.nextflow/plugins
  cp -r build/plugins/nf-amazon-2.2.1 ${HOME}/.nextflow/plugins/

upload:
  aws s3 rm --recursive s3://latch-public/nextflow-v2/.nextflow
  aws s3 cp --recursive --quiet $HOME/.nextflow s3://latch-public/nextflow-v2/.nextflow
  aws s3 cp --quiet nextflow s3://latch-public/nextflow-v2/nextflow

do-the-thing: build upload

