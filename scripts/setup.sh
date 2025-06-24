#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

sudo apt-get update
sudo apt-get install --yes \
	restic \
	ceph-common \
	libcephfs-dev \
	librbd-dev \
	librados-dev \
	snapd
sudo snap install microceph

sudo microceph cluster bootstrap
sudo microceph disk add loop,4G,3
sudo microceph status

sudo microceph.ceph osd pool create restic

sudo cp /var/snap/microceph/current/conf/ceph.conf /etc/ceph/ceph.conf
sudo cp /var/snap/microceph/current/conf/ceph.client.admin.keyring /etc/ceph/ceph.client.admin.keyring
sudo cp /var/snap/microceph/current/conf/ceph.keyring /etc/ceph/ceph.keyring
sudo chmod 644 /etc/ceph/ceph.conf /etc/ceph/ceph.client.admin.keyring /etc/ceph/ceph.keyring
