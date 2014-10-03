#!/bin/bash
#
# test single class with Gradle 
#
# Read more 
# http://www.gradle.org/docs/1.10/release-notes#executing-specific-tests-from-the-command-line
# http://stackoverflow.com/questions/18061774/run-single-integration-test-with-gradle
#

MODULE=${2:-''}

if [[ $# -eq 0 ]]; then
  echo 'usage: g-test <ClassToTest> [sub-project]'
  echo ''
  echo 'examples:'
  echo '  //select specific test method'
  echo '  gradle test --tests org.gradle.SomeTest.someFeature'
  echo ''
  echo '  //select specific test class'
  echo '  gradle test --tests org.gradle.SomeTest'
  echo ''
  echo '  //select all tests from package'
  echo '  gradle test --tests org.gradle.internal*'
  echo ''
  echo '  //select all ui test methods from integration tests by naming convention'
  echo '  gradle test --tests *IntegTest*ui*'
  echo ''
  echo '  //selecting tests from different test tasks'
  echo '  gradle test --tests *UiTest integTest --tests *WebTest*ui'
  echo ''
  exit 1
fi 

set -x
./gradlew -q $MODULE:test --tests $1
