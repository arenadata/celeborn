#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG ubuntu_image_repository=ubuntu
ARG ubuntu_image_tag=22.04
ARG jdk_package=openjdk-17-jdk
ARG custom_jdk_path
ARG additional_packages_repo
FROM ${ubuntu_image_repository}:${ubuntu_image_tag}

ARG ubuntu_image_repository
ARG ubuntu_image_tag
ARG jdk_package
ARG custom_jdk_path
ARG additional_packages_repo
# Set to "true" to additionally install network debug tools.
ARG install_debug_tools=false

ARG celeborn_uid=10006
ARG celeborn_gid=10006

ENV CELEBORN_HOME=/opt/celeborn
ENV CELEBORN_PREFER_JEMALLOC=true
ENV PATH=${PATH}:/opt/busybox:${CELEBORN_HOME}/sbin:${CELEBORN_HOME}/bin

RUN set -ex && \
    if [ -n "${additional_packages_repo}" ]; then \
        echo ${additional_packages_repo} > /etc/apt/sources.list.d/additional.list; \
    fi && \
    apt-get update && \
    apt-get install -y bash tini busybox procps krb5-user libjemalloc-dev ${jdk_package} && \
    if [ "${install_debug_tools}" = "true" ]; then \
        apt-get install -y bind9-utils telnet net-tools dnsutils; \
    fi && \
    apt-get upgrade -y && \
    if [ -n "${custom_jdk_path}" ]; then \
        update-alternatives --install /usr/bin/java java ${custom_jdk_path}/bin/java 100; \
        update-alternatives --install /usr/bin/javac javac ${custom_jdk_path}/bin/javac 100; \
    fi && \
    if [ -n "${additional_packages_repo}" ]; then \
        rm -f /etc/apt/sources.list.d/additional.list; \
    fi && \
    apt-get clean && \
    ln -snf /bin/bash /bin/sh && \
    rm -rf /var/cache/apt/* && \
    mkdir /opt/busybox && \
    busybox --install /opt/busybox && \
    groupadd --gid=${celeborn_gid} celeborn && \
    useradd  --uid=${celeborn_uid} --gid=${celeborn_gid} celeborn -d /home/celeborn -m && \
    mkdir -p ${CELEBORN_HOME} && \
    chown -R celeborn:celeborn ${CELEBORN_HOME} && \
    chmod -R ug+rw ${CELEBORN_HOME}

COPY --chmod=a+x bin /opt/celeborn/bin
COPY --chmod=a+x sbin /opt/celeborn/sbin
COPY conf /opt/celeborn/conf
COPY jars /opt/celeborn/jars
COPY master-jars /opt/celeborn/master-jars
COPY worker-jars /opt/celeborn/worker-jars
COPY cli-jars /opt/celeborn/cli-jars
COPY RELEASE /opt/celeborn/RELEASE

USER celeborn
WORKDIR /opt/celeborn

ENTRYPOINT ["/usr/bin/tini", "--"]
