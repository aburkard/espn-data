#!/usr/bin/env bash
#
# Archive raw and processed data into split tar.gz files for Git LFS.
#
# Usage:
#   ./scripts/archive_data.sh          # archive both raw and processed
#   ./scripts/archive_data.sh raw      # archive only raw
#   ./scripts/archive_data.sh processed # archive only processed
#
# This creates:
#   data/raw.tar.gz + data/raw.tar.gz.part.*
#   data/processed.tar.gz + data/processed.tar.gz.part.*
#
# Split size is 2GB to stay within Git LFS limits.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$(cd "$SCRIPT_DIR/../data" && pwd)"
SPLIT_SIZE="2g"

archive() {
    local name="$1"  # "raw" or "processed"
    local src_dir="$DATA_DIR/$name"
    local tar_file="$DATA_DIR/$name.tar.gz"

    if [ ! -d "$src_dir" ]; then
        echo "ERROR: Source directory not found: $src_dir"
        return 1
    fi

    echo "=== Archiving $name ==="
    echo "  Source: $src_dir"

    # Remove old archive and parts
    rm -f "$tar_file" "$tar_file".part.*

    # Create tar.gz (exclude .DS_Store), running from data/ so paths are relative
    echo "  Creating $tar_file ..."
    tar czf "$tar_file" -C "$DATA_DIR" --exclude='.DS_Store' "$name/"

    local size
    size=$(du -h "$tar_file" | cut -f1)
    echo "  Archive size: $size"

    # Split into parts
    echo "  Splitting into ${SPLIT_SIZE} parts ..."
    split -b "$SPLIT_SIZE" "$tar_file" "$tar_file.part."

    local part_count
    part_count=$(ls "$tar_file".part.* 2>/dev/null | wc -l | tr -d ' ')
    echo "  Created $part_count parts:"
    ls -lh "$tar_file".part.* | awk '{print "    " $NF ": " $5}'

    # Verify: recombine parts and compare checksum
    echo "  Verifying split integrity ..."
    local original_hash combined_hash
    original_hash=$(md5 -q "$tar_file")
    combined_hash=$(cat "$tar_file".part.* | md5 -q)

    if [ "$original_hash" = "$combined_hash" ]; then
        echo "  Checksum OK: $original_hash"
    else
        echo "  ERROR: Checksum mismatch!"
        echo "    Original:  $original_hash"
        echo "    Combined:  $combined_hash"
        return 1
    fi

    echo ""
}

# Determine what to archive
targets="${1:-both}"

case "$targets" in
    raw)
        archive "raw"
        ;;
    processed)
        archive "processed"
        ;;
    both)
        archive "raw"
        archive "processed"
        ;;
    *)
        echo "Usage: $0 [raw|processed|both]"
        exit 1
        ;;
esac

echo "Done. To commit to the data submodule:"
echo "  cd $DATA_DIR"
echo "  git add *.tar.gz *.tar.gz.part.*"
echo "  git commit -m 'Update compressed data files'"
echo "  git push"
