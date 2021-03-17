#!/bin/bash -e
base="$(dirname "$0")"

usage() {
  echo '##' "$1"
  shift
  echo '```sh'
  "$@"
  echo '```'
  echo
}

exec 1>"$base/../doc/usage.md"

echo '# Usage'
echo

usage common slicedb --help
usage dump slicedb dump --help
usage restore slicedb restore --help
usage schema slicedb schema --help
usage schema-filter slicedb schema-filter --help
usage transform slicedb transform --help
usage transform-field slicedb transform-field --help

"$base/../node_modules/.bin/prettier" --write "$base/../doc/usage.md"
