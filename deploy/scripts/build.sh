#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
build_dir="${project_dir}/build"
cmake -S "${project_dir}" -B "${build_dir}" -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build "${build_dir}" --parallel
