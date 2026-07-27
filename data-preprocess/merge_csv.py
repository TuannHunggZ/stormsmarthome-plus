import argparse
import heapq
import itertools
import os
import time


BUFFER_SIZE = 16 * 1024 * 1024  # 16 MB


def extract_timestamp(line: str) -> int:
    """
    Extract the timestamp (second column) without using split() for better performance.
    """
    first = line.find(",")
    second = line.find(",", first + 1)
    return int(line[first + 1:second])


def merge_files(input_dir: str, output_file: str, n: int):
    """
    Merge house-0.csv through house-(n-1).csv into a single
    timestamp-sorted output file.
    """

    files = []
    heap = []

    # Sequence number used to preserve insertion order
    # when multiple records have the same timestamp.
    sequence = itertools.count()

    start = time.time()
    records = 0

    try:
        # -----------------------------
        # Open all input files
        # -----------------------------
        for i in range(n):

            path = os.path.join(input_dir, f"house-{i}.csv")

            if not os.path.isfile(path):
                raise FileNotFoundError(path)

            f = open(path, "r", buffering=BUFFER_SIZE)

            files.append(f)

            line = f.readline()

            if line:

                timestamp = extract_timestamp(line)

                heapq.heappush(
                    heap,
                    (
                        timestamp,
                        next(sequence),
                        i,
                        line,
                    ),
                )

        # -----------------------------
        # Merge records
        # -----------------------------
        with open(
            output_file,
            "w",
            buffering=BUFFER_SIZE,
        ) as out:

            while heap:

                timestamp, _, file_index, line = heapq.heappop(heap)

                out.write(line)

                records += 1

                if records % 1_000_000 == 0:

                    elapsed = time.time() - start

                    speed = records / elapsed

                    print(
                        f"{records:>15,} records | "
                        f"{speed:>12,.0f} rec/s | "
                        f"timestamp={timestamp}"
                    )

                next_line = files[file_index].readline()

                if next_line:

                    next_timestamp = extract_timestamp(next_line)

                    heapq.heappush(
                        heap,
                        (
                            next_timestamp,
                            next(sequence),
                            file_index,
                            next_line,
                        ),
                    )

    finally:
        # Close all opened files.
        for f in files:
            f.close()

    elapsed = time.time() - start

    print("\nMerge completed")
    print(f"Output : {output_file}")
    print(f"Records: {records:,}")
    print(f"Elapsed: {elapsed:.2f} s")
    print(f"Speed  : {records / elapsed:,.0f} rec/s")


def main():

    parser = argparse.ArgumentParser(
        description="Merge DEBS 2014 house CSV files."
    )

    parser.add_argument(
        "-n",
        "--num-files",
        type=int,
        required=True,
        help="Merge house-0.csv ... house-(n-1).csv",
    )

    parser.add_argument(
        "-i",
        "--input-dir",
        default="../mqtt-publisher/data-file",
        help="Directory containing house-*.csv",
    )

    parser.add_argument(
        "-o",
        "--output",
        default="merged.csv",
        help="Output CSV file",
    )

    args = parser.parse_args()

    merge_files(
        input_dir=args.input_dir,
        output_file=args.output,
        n=args.num_files,
    )


if __name__ == "__main__":
    main()