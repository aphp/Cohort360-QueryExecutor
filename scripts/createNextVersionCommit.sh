#!/bin/bash

set -e

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)


# Check if the version is a new minor release (x.y.0)
if [[ $VERSION =~ ^([0-9]+)\.([0-9]+)\.0$ ]]; then
    MAJOR=${BASH_REMATCH[1]}
    MINOR=${BASH_REMATCH[2]}
    NEXT_MINOR_VERSION="$MAJOR.$((MINOR + 1)).0-SNAPSHOT"

    echo "Preparing for next development version: $NEXT_MINOR_VERSION"

    # Update version in pom.xml to the next minor version
    mvn versions:set -DnewVersion=$NEXT_MINOR_VERSION

    echo "You can review the modification and then execute this command to commit and push the changes:"
    echo "git add pom.xml && git commit -m \"build: prepare for next development version $NEXT_MINOR_VERSION\" && git push origin HEAD"

    read -p "Would you like to automatically execute these commands? (y/N) " answer
    if [ "$answer" = "y" ]; then
        # Add files and create commit
        git add .
        git commit -m "build: prepare for next development version $NEXT_MINOR_VERSION"
        git push origin HEAD
        echo "Next development version set and pushed successfully."
    else
        echo "Next development version not set."
    fi
else
    echo "Version $VERSION is not a new minor release. Skipping next development version setup."
fi
