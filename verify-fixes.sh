#!/usr/bin/env bash
# Re-runs the repro against local checkouts of Weasel and JasperFx instead of the released packages.
# Usage: bash verify-fixes.sh <weasel checkout> <jasperfx checkout>
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 <path to a weasel checkout> <path to a jasperfx checkout>" >&2
  exit 2
fi

absolute() { (cd "$1" && (pwd -W 2>/dev/null || pwd)); }
version_of() { sed -n "s:.*<$2>\(.*\)</$2>.*:\1:p" "$1" | head -1; }
required_by_polecat() { sed -n "s:.*<$1 Condition=[^>]*>\(.*\)</$1>.*:\1:p" "$here/tests/HotColdRepro.Tests/HotColdRepro.Tests.csproj" | head -1; }

# True when the dotted version $1 is at least $2
at_least() {
  local IFS=.
  local -a have=($1) want=($2)
  local i
  for ((i = 0; i < ${#have[@]} || i < ${#want[@]}; i++)); do
    local h=${have[i]:-0} w=${want[i]:-0}
    if ((10#$h > 10#$w)); then return 0; fi
    if ((10#$h < 10#$w)); then return 1; fi
  done
  return 0
}

here="$(absolute "$(dirname "$0")")"
weasel="$(absolute "$1")"
jasperfx="$(absolute "$2")"
feed="$here/.local-feed"
packages="$here/.local-packages"

for props in "$weasel/Directory.Build.props" "$jasperfx/Directory.Build.props"; do
  [ -f "$props" ] || { echo "$props not found; pass the root of a weasel and a jasperfx checkout" >&2; exit 2; }
done

weasel_base="$(version_of "$weasel/Directory.Build.props" Version)"
jasperfx_base="$(version_of "$jasperfx/Directory.Build.props" JasperFxVersion)"
weasel_required="$(required_by_polecat WeaselVersion)"
jasperfx_required="$(required_by_polecat JasperFxVersion)"

# Polecat depends on minimum versions of both, so an older checkout fails restore with a package downgrade
if ! at_least "$weasel_base" "$weasel_required"; then
  echo "The Weasel checkout is based on $weasel_base; rebase it onto $weasel_required or later." >&2
  exit 1
fi
if ! at_least "$jasperfx_base" "$jasperfx_required"; then
  echo "The JasperFx checkout is based on $jasperfx_base; rebase it onto $jasperfx_required or later." >&2
  exit 1
fi

# A fourth segment ranks above the release each checkout is based on, so Polecat's minimum versions still resolve to it
weasel_version="$weasel_base.9999"
jasperfx_version="$jasperfx_base.9999"

rm -rf "$feed" "$packages"
mkdir -p "$feed"

echo "Packing Weasel $weasel_version from $weasel ($(git -C "$weasel" rev-parse --abbrev-ref HEAD))"
for project in Weasel.Core Weasel.Storage Weasel.SqlServer; do
  dotnet pack "$weasel/src/$project/$project.csproj" -c Release -o "$feed" -p:Version="$weasel_version" --nologo -v q
done

echo "Packing JasperFx $jasperfx_version from $jasperfx ($(git -C "$jasperfx" rev-parse --abbrev-ref HEAD))"
for project in JasperFx JasperFx.Events; do
  dotnet pack "$jasperfx/src/$project/$project.csproj" -c Release -o "$feed" -p:JasperFxVersion="$jasperfx_version" --nologo -v q
done

# A private package cache, so these builds never shadow the released packages in the global one
NUGET_PACKAGES="$packages" dotnet test "$here/HotColdRepro.slnx" \
  -p:WeaselVersion="$weasel_version" \
  -p:JasperFxVersion="$jasperfx_version" \
  -p:RestoreAdditionalProjectSources="$feed"
