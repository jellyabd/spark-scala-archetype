#!/bin/bash
mvn archetype:create-from-project && pushd target/generated-sources/archetype && mvn install && popd
