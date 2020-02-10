#!/bin/bash

poetry build;

if [ $1 == 'develop' ]; then
    poetry config repositories.testpypi https://test.pypi.org/legacy/;
    poetry publish -r testpypi;
elif [ $1 == 'master' ]; then
  poetry publish;
fi
