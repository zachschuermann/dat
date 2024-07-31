import os
from dat.spark_builder import get_spark_session

tests = """124-decimal-decode-bug
125-iterator-bug
basic-decimal-table
basic-decimal-table-legacy
basic-with-inserts-deletes-checkpoint
basic-with-inserts-merge
basic-with-inserts-overwrite-restore
basic-with-inserts-updates
basic-with-vacuum-protocol-check-feature
canonicalized-paths-normal-a
canonicalized-paths-normal-b
canonicalized-paths-special-a
canonicalized-paths-special-b
checkpoint
corrupted-last-checkpoint
corrupted-last-checkpoint-kernel
data-reader-absolute-paths-escaped-chars
data-reader-array-complex-objects
data-reader-array-primitives
data-reader-date-types-America
data-reader-date-types-Asia
data-reader-date-types-Etc
data-reader-date-types-Iceland
data-reader-date-types-JST
data-reader-date-types-PST
data-reader-date-types-UTC
data-reader-escaped-chars
data-reader-map
data-reader-nested-struct
data-reader-nullable-field-invalid-schema-key
data-reader-partition-values
data-reader-primitives
data-reader-timestamp_ntz
data-reader-timestamp_ntz-id-mode
data-reader-timestamp_ntz-name-mode
data-skipping-basic-stats-all-types
data-skipping-basic-stats-all-types-checkpoint
data-skipping-basic-stats-all-types-columnmapping-id
data-skipping-basic-stats-all-types-columnmapping-name
data-skipping-change-stats-collected-across-versions
data-skipping-partition-and-data-column
decimal-various-scale-precision
delete-re-add-same-file-different-transactions
deltalog-commit-info
deltalog-getChanges
deltalog-invalid-protocol-version
deltalog-state-reconstruction-from-checkpoint-missing-metadata
deltalog-state-reconstruction-from-checkpoint-missing-protocol
deltalog-state-reconstruction-without-metadata
deltalog-state-reconstruction-without-protocol
dv-partitioned-with-checkpoint
dv-with-columnmapping
hive
kernel-timestamp-INT96
kernel-timestamp-PST
kernel-timestamp-TIMESTAMP_MICROS
kernel-timestamp-TIMESTAMP_MILLIS
log-replay-dv-key-cases
log-replay-latest-metadata-protocol
log-replay-special-characters
log-replay-special-characters-a
log-replay-special-characters-b
log-store-listFrom
log-store-read
multi-part-checkpoint
no-delta-log-folder
only-checkpoint-files
parquet-all-types
parquet-all-types-legacy-format
parquet-decimal-dictionaries
parquet-decimal-dictionaries-v1
parquet-decimal-dictionaries-v2
parquet-decimal-type
snapshot-data0
snapshot-data1
snapshot-data2
snapshot-data2-deleted
snapshot-data3
snapshot-repartitioned
snapshot-vacuumed
table-with-columnmapping-mode-id
table-with-columnmapping-mode-name
time-travel-partition-changes-a
time-travel-partition-changes-b
time-travel-schema-changes-a
time-travel-schema-changes-b
time-travel-start
time-travel-start-start20
time-travel-start-start20-start40
update-deleted-directory
v2-checkpoint-json
v2-checkpoint-parquet
versions-not-contiguous"""

skipped = [
    "canonicalized-paths-normal-a", # no columns
    "canonicalized-paths-normal-b", # no columns
    "canonicalized-paths-special-a", # no columns
    "canonicalized-paths-special-b", # no columns
    "checkpoint",
    # "corrupted-last-checkpoint", # no columns
    # "corrupted-last-checkpoint-kernel", # no columns
    # DeltaIllegalStateException: [DELTA_STATE_RECOVER_ERROR] The metadata of your Delta table
    # could not be recovered while Reconstructing version: 1. Did you manually delete files in the
    # _delta_log directory?
    "data-reader-absolute-paths-escaped-chars", # not used?
    "delete-re-add-same-file-different-transactions", # invalid table (no data)
    "deltalog-commit-info", # no columns
    "deltalog-invalid-protocol-version", # expected failure (reader version 99)
    "deltalog-state-reconstruction-from-checkpoint-missing-metadata", # expected failure
    "deltalog-state-reconstruction-from-checkpoint-missing-protocol", # expected failure
    "deltalog-state-reconstruction-without-metadata", # expected failure
    "deltalog-state-reconstruction-without-protocol", # expected failure
    "hive", # need to fix tests
    "log-replay-special-characters-b", # data file doesn't exist
    "log-store-listFrom", # weird test
    "log-store-read", # weird test
    "no-delta-log-folder", # expected failure
    "update-deleted-directory", # no data
    "versions-not-contiguous", # expected DELTA_VERSIONS_NOT_CONTIGUOUS
]

def write_expected(spark, test_name):
    df = spark.read.format("delta").load(f"/Users/zach.schuermann/dev/delta-kernel-rs/kernel/tests/golden_data/{test_name}/table")
    df.show()
    df.write.parquet(f"/Users/zach.schuermann/dev/delta-kernel-rs/kernel/tests/golden_data/{test_name}/expected/")

def main():
    spark = get_spark_session()
    # we want all golden data to be in microsecond format, as that's what delta is always
    # supposed to read
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
    tests = [
        ("kernel-timestamp-timestamp_micros", "UTC"),
        ("kernel-timestamp-timestamp_millis", "UTC"),
        ("kernel-timestamp-int96", "UTC"),
        ("kernel-timestamp-pst", "PST"),
    ]
    for (test, tz) in tests: #.split("\n"):
        print(f"test {test}:")
        if test in skipped or os.path.exists(f"/Users/zach.schuermann/dev/delta-kernel-rs/kernel/tests/golden_data/{test}/expected"):
            print("skipped.")
            continue
        write_expected(spark, test)
        print("done.")
        print()

if __name__ == '__main__':
    main()
