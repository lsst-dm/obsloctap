#!/bin/bash

# This script updates packages in the base Docker image that's used by both the
# build and runtime images, and gives us a place to install additional
# system-level packages with apt-get.
#
# Based on the blog post:
# https://pythonspeed.com/articles/system-packages-docker/

# Bash "strict mode", to help catch problems and bugs in the shell
# script. Every bash script you write should include this. See
# http://redsymbol.net/articles/unofficial-bash-strict-mode/ for
# details.
set -euo pipefail

# Display each command as it's run.
set -x

# Tell apt-get we're never going to be able to give manual
# feedback:
export DEBIAN_FRONTEND=noninteractive

# Debian bullseye is EOL and its packages have moved to archive.debian.org
# (deb.debian.org still serves stale indexes but 404s on the .deb files).
# Archived Release files are expired, so skip the Valid-Until check.
sed -i -e 's|deb.debian.org|archive.debian.org|g' \
    -e 's|security.debian.org|archive.debian.org|g' \
    /etc/apt/sources.list
echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99archive

# Update the package listing, so we know what packages exist:
apt-get update

# Install security updates:
apt-get -y upgrade

# Example of installing a new package, without unnecessary packages:
apt-get -y install --no-install-recommends git

# Delete cached files we don't need anymore:
apt-get clean
rm -rf /var/lib/apt/lists/*
