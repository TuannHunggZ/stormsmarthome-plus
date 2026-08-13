import argparse
import csv
import os
import random

SECONDS_PER_DAY = 86400
RANDOM_RANGE = 1.0


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


def write_shifted_day(input_file, days, output_file):
    """
    Generate the shifted current day (Day N+1).
    """

    offset = days * SECONDS_PER_DAY

    with open(input_file, "r", newline="") as fin, \
            open(output_file, "w", newline="") as fout:

        reader = csv.reader(fin)
        writer = csv.writer(fout)

        for row in reader:

            writer.writerow([
                int(row[0]),
                int(row[1]) + offset,
                f"{float(row[2]):.3f}",
                row[3],
                row[4],
                row[5],
                row[6],
            ])


def write_history(input_file, max_id, days, output_file):
    """
    Generate historical data.

    Output order:

        DayN
        DayN-1
        ...
        Day1
    """

    random.seed(42)

    current_id = max_id + 1

    with open(output_file, "w", newline="") as fout:

        writer = csv.writer(fout)

        # DayN -> Day1
        for day in range(days, 0, -1):

            offset = (day - 1) * SECONDS_PER_DAY

            print(f"Generating Day {day}...")

            with open(input_file, "r", newline="") as fin:

                reader = csv.reader(fin)

                for row in reader:

                    if row[3] != "1":
                        continue

                    value = max(
                        0.0,
                        float(row[2]) + random.uniform(
                            -RANDOM_RANGE,
                            RANDOM_RANGE,
                        ),
                    )

                    writer.writerow([
                        current_id,
                        int(row[1]) + offset,
                        f"{value:.3f}",
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                    ])

                    current_id += 1


def main():

    parser = argparse.ArgumentParser(
        description="Generate historical DEBS dataset."
    )

    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Input house CSV file",
    )

    parser.add_argument(
        "-d",
        "--days",
        type=int,
        required=True,
        help="Number of historical days",
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        required=True,
        help="Output directory",
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    basename = os.path.splitext(
        os.path.basename(args.input)
    )[0]

    shifted_file = os.path.join(
        args.output_dir,
        f"{basename}_day-{args.days + 1}.csv",
    )

    history_file = os.path.join(
        args.output_dir,
        f"historical_{basename}_day-1-{args.days}.csv",
    )

    print("Scanning input file...")
    max_id = find_max_id(args.input)

    print(f"Maximum ID: {max_id}")

    print("Generating shifted current day...")
    write_shifted_day(
        args.input,
        args.days,
        shifted_file,
    )

    print("Generating historical data...")
    write_history(
        args.input,
        max_id,
        args.days,
        history_file,
    )

    print("\nDone.")
    print(f"Shifted day : {shifted_file}")
    print(f"History     : {history_file}")


if __name__ == "__main__":
    main()