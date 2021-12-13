#!/bin/bash
LANGUAGES=('fr')
SHARDS=1
THRESHOLD=3
SHINGLE_SIZE=8
NUM_TREES=256
PYTHON="/home/chenghao/miniconda3/envs/py37/bin/python"
SCRIPT="/home/chenghao/data_tooling/ac_dc/deduplicate.py"

for lang in "${LANGUAGES[@]}"; do
    echo "lang: $lang"

    # 3h for unpacking

    # ┌───────────────────────────────────────────────────────┐
    # │                                                       │
    # │                                                       │
    # │It took 1.5h for hashing 160 GB French dataset, but it │
    # │   can be faster with shards processed in parallel.    │
    # │                                                       │
    # │                                                       │
    # └───────────────────────────────────────────────────────┘

    # if [[ $SHARDS -ne 1 ]]
    # then
    #     echo "Creating ${SHARDS} shards"
    #     $PYTHON $SCRIPT create-shards "cache/sharded_deduplicated_${lang}" $SHARDS --path "oscar-corpus/OSCAR-2109" --name "deduplicated_${lang}" --split "train"
    #     echo "Hashing documents"
    #     for i in $(seq -f "%05g" 0 "$((SHARDS - 1))"); do
    #         echo "Hashing shard ${i}"
    #         $PYTHON $SCRIPT build-hashes "cache/sharded_deduplicated_${lang}/hashes_${i}" --data-files "sharded_${i}.jsonl" --path "cache/sharded_deduplicated_${lang}" --split "train" --shingle-size $SHINGLE_SIZE
    #     done
    # else
    #     echo "Hashing documents"
    #     for i in $(seq -f "%05g" 0 "$((SHARDS - 1))"); do
    #         echo "Hashing shard ${i}"
    #         mkdir -p "cache/sharded_deduplicated_${lang}"
    #         $PYTHON $SCRIPT build-hashes "cache/sharded_deduplicated_${lang}/hashes_${i}" --path "oscar-corpus/OSCAR-2109" --name "deduplicated_${lang}" --split "train" --shingle-size $SHINGLE_SIZE
    #     done
    # fi

    # ┌───────────────────────────────────────────────────────┐
    # │                                                       │
    # │              It took 50 min for French.               │
    # │                                                       │
    # │                                                       │
    # └───────────────────────────────────────────────────────┘
    # echo "Gather hashes"
    # $PYTHON $SCRIPT gather-hashes "cache/sharded_deduplicated_${lang}/simhash_vectors.hdf5" $(seq -s " " -f "cache/sharded_deduplicated_${lang}/hashes_%05g" 0 "$((SHARDS - 1))") --split "train"
    
    # ┌─────────────────────────────────────────────────────────────────────────┐
    # │                                                                         │
    # │                                                                         │
    # │                                                                         │
    # │                         10 minutes for indexing                         │
    # │           few hours for querying, depending on the parameters           │
    # │                                                                         │
    # │                                                                         │
    # │                                                                         │
    # └─────────────────────────────────────────────────────────────────────────┘
    echo "Finding duplicates"
    for i in $(seq -f "%05g" 0 "$((SHARDS - 1))"); do
        echo "Querying shard ${i}"
        $PYTHON -W ignore $SCRIPT find-scann-duplicates "cache/sharded_deduplicated_${lang}/hashes_${i}" "cache/sharded_deduplicated_${lang}/simhash_vectors.hdf5" --split "train" --k 50 --threshold $THRESHOLD
    done

    echo "Removing duplicates"
    for i in $(seq -f "%05g" 0 "$((SHARDS - 1))"); do
        echo "Cleaning shard ${i}"
        $PYTHON $SCRIPT remove-duplicates "cache/sharded_deduplicated_${lang}/hashes_${i}_duplicates" --split "train"
    done

    echo "Merging shards"
    $PYTHON $SCRIPT merge-shards "cache/sharded_deduplicated_${lang}/output" $(seq -s " " -f "cache/sharded_deduplicated_${lang}/hashes_%05g_deduplicated" 0 "$((SHARDS - 1))") --split "train"

    echo "Done"
done
