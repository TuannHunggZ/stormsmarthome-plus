import argparse
import csv
import os


DAY_END_TIMESTAMP = 1378072799


def find_max_id(input_file):
    """
    Scan the input file and return the maximum ID.
    """

    max_id = 0

    with open(input_file, "r", newline="") as f:
        reader = csv.reader(f)

        for row in reader:
            max_id = max(max_id, int(row[0]))

    return max_id


def find_timestamp_range(input_file):
    """
    Scan the input file and return the minimum
    and maximum timestamp.
    """

    min_timestamp = None
    max_timestamp = None

    with open(input_file, "r", newline="") as f:
        reader = csv.reader(f)

        for row in reader:
            timestamp = int(row[1])

            if min_timestamp is None:
                min_timestamp = timestamp
                max_timestamp = timestamp
                continue

            min_timestamp = min(
                min_timestamp,
                timestamp,
            )

            max_timestamp = max(
                max_timestamp,
                timestamp,
            )

    if min_timestamp is None:
        raise ValueError("Input file is empty.")

    return min_timestamp, max_timestamp


def write_full_day(
    input_file,
    min_timestamp,
    max_timestamp,
    max_id,
    output_file,
):
    """
    Generate missing data to complete the target time range.

    The generated data is copied from the original dataset
    and shifted by the duration of the original timestamp range.

    Values and all other fields remain unchanged.

    Only:
        - ID
        - timestamp

    are changed for generated rows.
    """

    # Duration covered by the original dataset.
    duration = (
        max_timestamp - min_timestamp + 1
    )

    print(f"Minimum timestamp : {min_timestamp}")
    print(f"Maximum timestamp : {max_timestamp}")
    print(f"Duration          : {duration} seconds")

    current_id = max_id + 1
    generated_rows = 0

    with open(input_file, "r", newline="") as fin, \
            open(output_file, "w", newline="") as fout:

        reader = csv.reader(fin)
        writer = csv.writer(fout)

        # ---------------------------------
        # Write original data
        # ---------------------------------

        for row in reader:
            writer.writerow(row)

        # ---------------------------------
        # Read original data again
        # ---------------------------------

        fin.seek(0)
        reader = csv.reader(fin)

        # ---------------------------------
        # Generate missing data
        # ---------------------------------

        for row in reader:

            timestamp = int(row[1])

            # Shift timestamp by the duration
            # of the original dataset.
            new_timestamp = timestamp + duration

            # Do not generate data beyond
            # the target end timestamp.
            if new_timestamp > DAY_END_TIMESTAMP:
                break

            new_row = [
                current_id,
                new_timestamp,
                row[2],  # value
                row[3],  # property
                row[4],  # plug_id
                row[5],  # household_id
                row[6],  # house_id
            ]

            writer.writerow(new_row)

            current_id += 1
            generated_rows += 1

    print()
    print("========== Result ==========")
    print(f"Day end timestamp : {DAY_END_TIMESTAMP}")
    print(f"Duration          : {duration} seconds")
    print(f"Original max ID   : {max_id}")
    print(f"First generated ID: {max_id + 1}")
    print(f"Last generated ID : {current_id - 1}")
    print(f"Generated rows    : {generated_rows}")


def main():

    parser = argparse.ArgumentParser(
        description="Generate DEBS dataset to a full 24-hour period."
    )

    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Input CSV file",
    )

    parser.add_argument(
        "-o",
        "--output",
        help="Output CSV file",
    )

    args = parser.parse_args()

    if args.output is None:
        basename = os.path.splitext(
            os.path.basename(args.input)
        )[0]

        args.output = os.path.join(
            "data-file",
            f"{basename}_full.csv",
        )

    output_dir = os.path.dirname(args.output)

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    print("Scanning input file...")

    max_id = find_max_id(args.input)

    print(f"Maximum ID: {max_id}")

    min_timestamp, max_timestamp = (
        find_timestamp_range(args.input)
    )

    print(f"Minimum timestamp: {min_timestamp}")
    print(f"Maximum timestamp: {max_timestamp}")

    print()
    print("Generating full 24-hour data...")

    write_full_day(
        args.input,
        min_timestamp,
        max_timestamp,
        max_id,
        args.output,
    )

    print()
    print("Done.")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()