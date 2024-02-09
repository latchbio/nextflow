do-the-thing:
  make clean
  make compile
  make install
  aws s3 cp --recursive --quiet $HOME/.nextflow s3://latch-public/.nextflow
