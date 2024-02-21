build:
  make clean
  make compile
  make install

# upload:
#   aws s3 cp --recursive --quiet $HOME/.nextflow s3://latch-public/.nextflow
#   aws s3 cp --quiet nextflow s3://latch-public/nextflow

# do-the-thing: build upload