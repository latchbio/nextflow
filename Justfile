build:
  make clean
  make compile
  make install

upload:
  aws s3 rm --recursive s3://latch-public/nextflow-v2/.nextflow
  aws s3 cp --recursive --quiet $HOME/.nextflow s3://latch-public/nextflow-v2/.nextflow
  aws s3 cp --quiet nextflow s3://latch-public/nextflow-v2/nextflow

do-the-thing: build upload
