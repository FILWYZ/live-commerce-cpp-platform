#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
build_dir="${project_dir}/build"
seconds="${FLASH_SOAK_SECONDS:-30}"

cmake -S "${project_dir}" -B "${build_dir}" -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build "${build_dir}" --target flash_sale_soak_test --parallel
FLASH_SOAK_SECONDS="${seconds}" "${build_dir}/flash_sale_soak_test"
