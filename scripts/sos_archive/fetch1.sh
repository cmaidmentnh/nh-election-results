#!/bin/bash
# fetch one archived page (nearest snapshot to 2012) into raw/<folder>/<file>
u="$1"; cd "$(dirname "$0")"
rel=${u#http://www.sos.nh.gov/}; out="raw/$(echo "$rel" | sed 's/%20/_/g')"
[ -s "$out" ] && grep -qv "temporarily offline" "$out" && exit 0
mkdir -p "$(dirname "$out")"
for i in 1 2 3 4 5; do
  curl -sL -m 120 -o "$out" "https://web.archive.org/web/2012id_/$u"
  if [ -s "$out" ] && ! grep -q "Temporarily Offline" "$out"; then exit 0; fi
  rm -f "$out"; sleep $((i*6))
done
echo "FAILED $u"
