#!/bin/bash

set -e

# We could also use instead an arg BUMP_TYPE which would be either "patch" or "minor"
# and then use return value of git cliff to get the new version number
# ```
# `git cliff --bump $BUMP_TYPE --unreleased > temp_changelog.md`
# VERSION=$(cat temp_changelog.md | grep -oP '## \[\K[^\]]+')
# ```

# Exit if no version argument provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <version>"
    exit 1
fi

VERSION=$1

echo "Creating release commit for version $VERSION"

# Update version in package.json and package-lock.json
mvn versions:set -DnewVersion=$VERSION

# Generate changelog and prepend to CHANGELOG.md
git fetch --tags # ensure all the tags are fetched
git cliff --tag $VERSION --unreleased > temp_changelog.md
tail -n +4 CHANGELOG.md >> temp_changelog.md
mv temp_changelog.md CHANGELOG.md

echo "You can review the modification and then execute this command to commit and push the changes:"
echo "git add package.json package-lock.json CHANGELOG.md && git commit -m \"build: set release version $VERSION\" && git tag $VERSION && git push origin $VERSION && git push origin HEAD"

read -p "Would you like to automatically execute these commands? (y/N) " answer
if [ "$answer" = "y" ]; then
  # Add files and create commit
    git add pom.xml CHANGELOG.md
    git commit -m "build: set release version $VERSION"
    git tag $VERSION
    git push origin $VERSION
    git push origin HEAD
    echo "Release commit created and pushed successfully."
else
    echo "Release commit not created."
    exit 0
fi

# Prepare for next development version
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
